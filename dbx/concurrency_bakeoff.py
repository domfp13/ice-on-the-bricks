# python concurrency_bakeoff.py databricks --query-mode query1
# python concurrency_bakeoff.py snowflake --query-mode query2
# python concurrency_bakeoff.py --show --query-mode query1

# pip install matplotlib
# pip install python-dotenv
# pip install pyarrow
# pip install snowflake-connector-python
# pip install databricks-sql-connector

# pip install matplotlib python-dotenv pyarrow snowflake-connector-python databricks-sql-connector

import time, statistics, argparse, os
from concurrent.futures import ThreadPoolExecutor
import matplotlib as mpl
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv() 

# ---------- connection helpers ----------
def snowflake_connect():
    import snowflake.connector
    return snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASS"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse="MASTERCLASSWH",
        database="MASTERCLASS",
        schema="PUBLIC",   
    )

def databricks_connect():
    from databricks import sql
    return sql.connect(
        server_hostname=os.getenv("SERVER_HOSTNAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("ACCESS_TOKEN"),
        # Avoid Arrow/pandas conversion path that can segfault when mixed with
        # other native connectors in the same process.
        use_cloud_fetch=False,
        _disable_pandas=True,
    )

#TABLE = {"snowflake": "MASTERCLASS.PUBLIC.LINEITEMS_WAREHOUSE", "databricks": "04_CONCURRENCY.DEFAULT.LINEITEMS_WAREHOUSE"}
TABLE = {"snowflake": "MASTERCLASS.PUBLIC.LINEITEMS_WAREHOUSE_ICEBERG", "databricks": "05_CONCURRENCY_ICEBERG.DEFAULT.LINEITEMS_WAREHOUSE"}


def build_query_query1(table):
    # query1 = fixed cutoff date, preserving original benchmark behavior.
    return f"""
    SELECT l_returnflag,
        l_linestatus,
        SUM(l_quantity)                          AS total_qty,
        SUM(l_extendedprice*(1-l_discount))      AS total_revenue,
        AVG(l_discount)                          AS avg_discount,
        COUNT(*)                                 AS row_cnt
    FROM   {table}
    WHERE  l_shipdate <= DATE '1998-09-02'
    GROUP  BY l_returnflag, l_linestatus
"""


def build_query_query2(kind, table):
    # query2 = dynamic cutoff date generated in SQL from table min/max range.
    if kind == "snowflake":
        return f"""
    WITH bounds AS (
        SELECT MIN(l_shipdate) AS min_shipdate,
               MAX(l_shipdate) AS max_shipdate
        FROM {table}
    ),
    cutoff AS (
        SELECT DATEADD(
                   day,
                   ABS(MOD(RANDOM(), DATEDIFF(day, min_shipdate, max_shipdate) + 1)),
                   min_shipdate
               ) AS cutoff_date
        FROM bounds
    )
    SELECT l_returnflag,
           l_linestatus,
           SUM(l_quantity)                     AS total_qty,
           SUM(l_extendedprice * (1-l_discount)) AS total_revenue,
           AVG(l_discount)                     AS avg_discount,
           COUNT(*)                            AS row_cnt
    FROM {table}
    WHERE l_shipdate <= (SELECT cutoff_date FROM cutoff)
    GROUP BY l_returnflag, l_linestatus
"""

    return f"""
    WITH bounds AS (
        SELECT MIN(l_shipdate) AS min_shipdate,
               MAX(l_shipdate) AS max_shipdate
        FROM {table}
    ),
    cutoff AS (
        SELECT date_add(
                   min_shipdate,
                   CAST(FLOOR(rand() * (datediff(max_shipdate, min_shipdate) + 1)) AS INT)
               ) AS cutoff_date
        FROM bounds
    )
    SELECT l_returnflag,
           l_linestatus,
           SUM(l_quantity)                     AS total_qty,
           SUM(l_extendedprice * (1-l_discount)) AS total_revenue,
           AVG(l_discount)                     AS avg_discount,
           COUNT(*)                            AS row_cnt
    FROM {table}
    WHERE l_shipdate <= (SELECT cutoff_date FROM cutoff)
    GROUP BY l_returnflag, l_linestatus
"""


def build_query(kind, query_mode):
    table = TABLE[kind]
    if query_mode == "query2":
        return build_query_query2(kind, table)
    return build_query_query1(table)

# ---------- runner ----------
def run_once(conn, sql_text):
    cur = conn.cursor()
    try:
        t0 = time.perf_counter()
        cur.execute(sql_text)
        cur.fetchall()          # force completion
        return time.perf_counter() - t0
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

def exercise(kind, concurrency, query_mode):
    connect = snowflake_connect if kind == "snowflake" else databricks_connect
    sql_text = build_query(kind, query_mode)
    durations = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(lambda: run_once(connect(), sql_text))
            for _ in range(concurrency)
        ]
        for f in futures:
            durations.append(f.result())
    return durations


# ---------- charting helper ----------
def safe_quantile(times, n, idx):
    if len(times) < 2:
        return float('nan')
    try:
        return statistics.quantiles(times, n=n)[idx]
    except Exception:
        return float('nan')

def plot_results(results, output_path=None, show=False):
    # Use a non-interactive backend when not showing to avoid blocking the process
    if not show:
        try:
            mpl.use('Agg')
        except Exception:
            pass

    # import pyplot after backend selection
    import matplotlib.pyplot as plt

    users = sorted(next(iter(results.values())).keys())
    fig, ax = plt.subplots(figsize=(10, 6))
    for kind, data in results.items():
        p50 = [statistics.median(data[u]) if data[u] else float('nan') for u in users]
        p95 = [safe_quantile(data[u], 20, 18) for u in users]
        maxv = [max(data[u]) if data[u] else float('nan') for u in users]
        ax.plot(users, p50, marker='o', label=f'{kind} p50')
        ax.plot(users, p95, marker='x', linestyle='--', label=f'{kind} p95')
        ax.plot(users, maxv, marker='s', linestyle=':', label=f'{kind} max')
    ax.set_xlabel('Concurrent Users')
    ax.set_ylabel('Query Time (s)')
    ax.set_title('Concurrency Benchmark: Snowflake vs Databricks')
    ax.legend()
    ax.grid(True)
    plt.tight_layout()

    # Save if requested
    if output_path:
        try:
            fig.savefig(output_path)
            print(f"Saved plot to {output_path}")
        except Exception as e:
            print(f"Failed to save plot: {e}")

    # Show interactively if requested (this will block)
    if show:
        plt.show()
    else:
        # Close the figure to free resources and avoid blocking
        plt.close(fig)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("platforms_pos", nargs="*", choices=["snowflake", "databricks"],
                        help="Platforms to benchmark (positional). Example: databricks")
    parser.add_argument("--platforms", dest="platforms_opt", nargs="*", choices=["snowflake", "databricks"], default=None,
                        help="Platforms to benchmark (default: both)")
    parser.add_argument("--users", nargs="*", type=int, default=[1, 8, 16, 32, 64],
                        help="List of concurrent user counts")
    parser.add_argument("--query-mode", choices=["query1", "query2"], default="query1",
                        help="Query mode: query1=fixed date, query2=dynamic random date")
    parser.add_argument("--output", default=None,
                        help="File path to save the plot (PNG). If not provided a timestamped file will be created")
    parser.add_argument("--show", action="store_true",
                        help="Show the plot interactively (will block until closed)")
    args = parser.parse_args()

    if args.platforms_opt is not None:
        selected_platforms = args.platforms_opt or ["snowflake", "databricks"]
    else:
        selected_platforms = args.platforms_pos or ["snowflake", "databricks"]

    print(f"\nSelected query mode: {args.query_mode} ({'fixed' if args.query_mode == 'query1' else 'dynamic'})")

    results = {kind: {} for kind in selected_platforms}
    for kind in selected_platforms:
        print(f"\n--- {kind.upper()} ---")
        for users in args.users:
            try:
                times = exercise(kind, users, args.query_mode)
            except Exception as e:
                print(f"{users:>2} users  →  ERROR: {e}")
                times = []
            results[kind][users] = times
            if times:
                p50 = statistics.median(times)
                p95 = safe_quantile(times, 20, 18)
                maxv = max(times)
                print(f"{users:>2} users  →  p50 {p50:5.2f}s  "
                      f"p95 {p95:5.2f}s  "
                      f"max {maxv:5.2f}s  "
                      f"(n={len(times)})")
            else:
                print(f"{users:>2} users  →  No data")
    # determine output path
    out = args.output
    if not out and not args.show:
        ts = time.strftime('%Y%m%d-%H%M%S')
        out = os.path.join(os.getcwd(), f'concurrency_bakeoff_{ts}.png')

    plot_results(results, output_path=out, show=args.show)
