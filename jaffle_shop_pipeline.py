import os
from itertools import product
from typing import Iterator

import dlt
import orjson
from dlt.common.typing import TDataItem
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
from loguru import logger
from tqdm import tqdm

@dlt.source
def jaffle_api_source(
    yield_page: bool = False,
    parallelized: bool = False,
    page_size: int = 5000,
    extract_workers: int = 1,
    normalize_workers: int = 1,
    load_workers: int = 1,
    **kwargs,
):
    os.environ["RUNTIME__LOG_LEVEL"] = "WARNING"
    os.environ["EXTRACT__WORKERS"] = str(kwargs.get("extract_workers", extract_workers))
    os.environ["NORMALIZE__WORKERS"] = str(
        kwargs.get("normalize_workers", normalize_workers)
    )
    os.environ["LOAD__WORKERS"] = str(kwargs.get("load_workers", load_workers))
    parallelized = kwargs.get("parallelized", parallelized)
    page_size = kwargs.get("page_size", page_size)
    yield_page = kwargs.get("yield_page", yield_page)

    client = RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1",
        paginator=HeaderLinkPaginator(),
    )

    @dlt.resource(name="customers", primary_key="id", parallelized=parallelized)
    def customers() -> Iterator[TDataItem]:
        for page in client.paginate(
            path="/customers",
            paginator=HeaderLinkPaginator(),
            params={"page_size": page_size},
        ):
            if yield_page:
                yield page
            else:
                yield from page

    @dlt.resource(name="orders", primary_key="id", parallelized=parallelized)
    def orders() -> Iterator[TDataItem]:
        for page in client.paginate(path="/orders", params={"page_size": page_size}):
            if yield_page:
                yield page
            else:
                yield from page

    @dlt.resource(name="products", primary_key="sku", parallelized=parallelized)
    def products() -> Iterator[TDataItem]:
        for page in client.paginate(
            path="/products",
            params={"page_size": page_size},
        ):
            if yield_page:
                yield page
            else:
                yield from page

    orders.add_limit(max_items=1000)
    return [customers, products]


def param_grid(grid: dict):
    keys = list(grid.keys())
    values = [
        grid[k] if isinstance(grid[k], (list, tuple)) else [grid[k]] for k in keys
    ]
    for combo in product(*values):
        val = dict(zip(keys, combo))
        if (
            val["parallelized"] == True and val["extract_workers"] > 1
        ):  # Causes threading error
            continue
        yield val


def load_jaffle():
    pipeline = dlt.pipeline(pipeline_name="jaffle", destination="duckdb")
    grid = {
        "yield_page": [True, False],
        "parallelized": [True, False],
        "page_size": [1000, 5000],
        "extract_workers": [1, 2],
        "normalize_workers": [1, 4],
        "load_workers": [1, 4],
    }
    metrics = []
    for params in tqdm(param_grid(grid)):
        logger.info(f"Running with params: {params}")

        run_info = pipeline.run(
            jaffle_api_source(**params), write_disposition="replace"
        )

        trace = pipeline.last_trace

        ext = trace.last_extract_info
        ext_duration = ext.finished_at - ext.started_at
        norm = trace.last_normalize_info
        norm_duration = norm.finished_at - norm.started_at
        ld = trace.last_load_info
        ld_duration = ld.finished_at - ld.started_at

        run_metrics = {
            **params,
            "ext_duration": ext_duration.total_seconds(),
            "norm_duration": norm_duration.total_seconds(),
            "ld_duration": ld_duration.total_seconds(),
        }
        metrics.append(run_metrics)
    return metrics


if __name__ == "__main__":
    metrics = load_jaffle()
    with open("jaffle_metrics.json", "wb") as f:
        f.write(orjson.dumps(metrics, option=orjson.OPT_INDENT_2))