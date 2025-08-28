import requests
import re
import urllib.parse
import json
import uuid
import sqlalchemy
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
# from H5DataBase import H5DataBase
from datetime import datetime
from typing import Literal, List
from typing import List
from pyecharts.charts import Line
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode
from pathlib import Path
from config import SQL_PASSWORDS, SQL_HOST

__all__ = [
    "load_bais",
    "now_time",
    "pivoted_df_insert_rows",
    "load_hist_data",
    "load_bench_cons",
    "calculate_percentile",
    "load_speed_of_indus",
    "load_speed_of_barra",
]

def connect_to_database(db_name = "UpdatedData"):
    """创建并返回数据库引擎"""
    print(f"连接到数据库{db_name}...")
    # 数据库连接
    engine = sqlalchemy.create_engine(
        f"mysql+pymysql://dev:{SQL_PASSWORDS}@{SQL_HOST}:3306/{db_name}?charset=utf8"
    )
    return engine


def now_time():
    return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")


# Function to calculate rolling percentile
def calculate_percentile(data: np.ndarray, window_size: int) -> np.ndarray:
    data_windows = np.lib.stride_tricks.as_strided(
        data, (len(data) - window_size + 1, window_size), (data.itemsize, data.itemsize)
    )
    percentile = (
        np.sum(data_windows <= data_windows[:, -1][:, None], axis=1)
        / data_windows.shape[1]
    )
    return percentile


def pivoted_df_insert_rows(exit_df: pd.DataFrame, add_df: pd.DataFrame):
    combined_index = exit_df.index.union(add_df.index)
    combined_columns = exit_df.columns.union(add_df.columns)
    add_df_values = add_df.reindex(
        index=combined_index,
        columns=combined_columns,
        fill_value=np.nan,
    ).values
    exit_df_values = exit_df.reindex(
        index=combined_index,
        columns=combined_columns,
        fill_value=np.nan,
    ).values

    return pd.DataFrame(
        np.where(np.isnan(add_df_values), exit_df_values, add_df_values),
        index=combined_index,
        columns=combined_columns,
    )


def load_bench_cons(
    bench_symbol: Literal["000985.CSI", "000300.SH", "000905.SH", "000852.SH"],
):
    cons_df = pd.read_csv(Path(f"data/cons_of_{bench_symbol}.csv"))
    return cons_df["wind_code"].to_list()


def load_hist_data_from_wind(
    indicator: Literal["CLOSE", "PCT_CHG", "VOLUME", "AMT"],
    symols: List[str] = None,
    start_date: np.datetime64 = None,
    end_date: np.datetime64 = None,
    raw_data_path: Path = Path(r"data/bench_cons_rtn_from_wind.csv"),
):
    data = pd.read_csv(raw_data_path)
    data = data.drop_duplicates(subset=["date", "code"], keep="last")
    data["date"] = pd.to_datetime(data["date"])
    data = data[data["date"] != np.datetime64("2025-03-01")]
    if start_date is not None:
        data = data[data["date"] >= start_date]
    if end_date is not None:
        data = data[data["date"] <= end_date]
    if symols is not None:
        data = data[data["code"].isin(symols)]
    return data.pivot(index="date", columns="code", values=indicator)


def load_hist_data(
    indicator: Literal["rtn", "close", "volume", "volumeRmb"],
    symols: List[int] = None,
    start_date: np.datetime64 = None,
    end_date: np.datetime64 = None,
    hist_985_path: Path = Path(r"data/demo.h5"),
):
    h5db = H5DataBase(hist_985_path)
    data = h5db.load_pivotDF_from_h5data(indicator)
    h5db.f_object_handle.close()
    if symols is not None:
        data = data[symols].copy()
    if start_date is not None:
        data = data[data.index >= start_date]
    if end_date is not None:
        data = data[data.index <= end_date]
    return data


def load_rtn_data_from_wind(
    symols: List[int] = None,
    start_date: np.datetime64 = None,
    end_date: np.datetime64 = None,
):
    if symols is not None:
        data = data[symols].copy()
    if start_date is not None:
        data = data[data.index >= start_date]
    if end_date is not None:
        data = data[data.index <= end_date]
    return data


def load_bais(type=Literal["IF", "IC", "IM", "IH"]) -> pd.DataFrame:
    if type == "IF":
        data = "params=%7B%22head%22%3A%22IF%22%2C%22N%22%3A251%7D&PageID=46803&websiteID=20906&ContentID=Content&UserID=&menup=0&_cb=&_cbdata=&_cbExec=1&_cbDispType=1&__pageState=0&__globalUrlParam=%7B%22PageID%22%3A%2246803%22%2C%22pageid%22%3A%2246803%22%7D&g_randomid=randomid_1051095574548506702800710985&np=%5B%2246803%40Content%40TwebCom_div_1_0%40220907102451613%22%5D&modename=amljaGFfZGFpbHlfY2hhcnRfN0Q5MTQ5NDE%3D&creator=cjzq"
    elif type == "IC":
        data = "params=%7B%22head%22%3A%22IC%22%2C%22N%22%3A251%7D&PageID=46803&websiteID=20906&ContentID=Content&UserID=&menup=0&_cb=&_cbdata=&_cbExec=1&_cbDispType=1&__pageState=0&__globalUrlParam=%7B%22PageID%22%3A%2246803%22%2C%22pageid%22%3A%2246803%22%7D&g_randomid=randomid_1051095574548506702800710985&np=%5B%2246803%40Content%40TwebCom_div_1_0%40220907102451613%22%5D&modename=amljaGFfZGFpbHlfY2hhcnRfN0Q5MTQ5NDE%3D&creator=cjzq"
    elif type == "IM":
        data = "params=%7B%22head%22%3A%22IM%22%2C%22N%22%3A251%7D&PageID=46803&websiteID=20906&ContentID=Content&UserID=&menup=0&_cb=&_cbdata=&_cbExec=1&_cbDispType=1&__pageState=0&__globalUrlParam=%7B%22PageID%22%3A%2246803%22%2C%22pageid%22%3A%2246803%22%7D&g_randomid=randomid_1051095574548506702800710985&np=%5B%2246803%40Content%40TwebCom_div_1_0%40220907102451613%22%5D&modename=amljaGFfZGFpbHlfY2hhcnRfN0Q5MTQ5NDE%3D&creator=cjzq"
    elif type == "IH":
        data = "params=%7B%22head%22%3A%22IH%22%2C%22N%22%3A251%7D&PageID=46803&websiteID=20906&ContentID=Content&UserID=&menup=0&_cb=&_cbdata=&_cbExec=1&_cbDispType=1&__pageState=0&__globalUrlParam=%7B%22PageID%22%3A%2246803%22%2C%22pageid%22%3A%2246803%22%7D&g_randomid=randomid_1051095574548506702800710985&np=%5B%2246803%40Content%40TwebCom_div_1_0%40220907102451613%22%5D&modename=amljaGFfZGFpbHlfY2hhcnRfN0Q5MTQ5NDE%3D&creator=cjzq"
    else:
        raise ValueError("type must be one of 'IF', 'IC', 'IM', 'IH'")
    decoded_data = urllib.parse.unquote(data)
    # 解析为字典格式
    parsed_params = urllib.parse.parse_qs(decoded_data)
    parsed_params["g_randomid"] = "randomid_" + str(uuid.uuid4().int)[:-11]
    updated_data = urllib.parse.urlencode(parsed_params, doseq=True)
    response = requests.post(
        "https://web.tinysoft.com.cn/website/loadContentDataAjax.tsl?ref=js",
        updated_data,
    )

    data = response.content.decode("utf-8", "ignore")
    data = json.loads(data)
    soup = BeautifulSoup(data["content"][0]["html"], "html.parser")
    script_content = soup.find("script").string
    match = re.search(r"var\s+SrcData\s*=\s*(\[.*?\]);", script_content, re.DOTALL)
    src_data_raw = match.group(1)
    # 将转义字符转换为实际字符
    src_data = json.loads(src_data_raw.encode().decode("unicode_escape"))
    data_df = pd.DataFrame(src_data)[
        [
            "日期",
            "主力合约",
            "期货价格",
            "现货价格",
            "基差",
            "到期日",
            "剩余天数",
            "期内分红",
            "矫正基差",
            "主力年化基差(%)",
            "年化基差(%)",
        ]
    ]
    # data_df["日期"] = pd.to_datetime(data_df["日期"])
    return data_df


# Function to generate a line chart
def plot_line_chart(
    x_data: np.ndarray,
    y_data: np.ndarray,
    name: str,
    range_start: int = 0,
    range_end: int = 100,
):
    line = (
        Line(
            init_opts={
                "width": "1560px",
                "height": "600px",
                "is_horizontal_center": True,
            }
        )
        .add_xaxis(list(x_data))
        .add_yaxis(name, list(y_data), is_symbol_show=False)
        .set_global_opts(
            xaxis_opts=opts.AxisOpts(type_="category"),
            yaxis_opts=opts.AxisOpts(type_="value"),
            legend_opts=opts.LegendOpts(
                textstyle_opts=opts.TextStyleOpts(font_weight="bold", font_size=17)
            ),
            datazoom_opts=[
                opts.DataZoomOpts(
                    range_start=range_start, range_end=range_end, orient="horizontal"
                )
            ],
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
        )
        .set_series_opts(
            linestyle_opts=opts.LineStyleOpts(width=3),
        )
    )
    return line

from pyecharts import options as opts
from pyecharts.charts import Line
from typing import List
import numpy as np


def plot_stacked_area_chart(
    x_data: np.ndarray,
    ys_data: List[np.ndarray],
    names: List[str],
    title: str = "",
    subtitle: str = "",
):
    """
    Plots a stacked area chart showing the contribution of each series to a total.
    """
    assert len(ys_data) == len(names), "Length of ys_data and names should be the same"

    line = (
        Line(init_opts={"width": "1560px", "height": "600px"})
        .add_xaxis(xaxis_data=list(x_data))
        .set_global_opts(
            title_opts=opts.TitleOpts(
                title=title, subtitle=subtitle, pos_left="center"
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="cross",
            ),
            legend_opts=opts.LegendOpts(pos_top="bottom"),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
    )

    # The loop is slightly different
    for i, y_data in enumerate(ys_data):
        line.add_yaxis(
            series_name=names[i],
            y_axis=list(y_data),
            # --- KEY CHANGES ARE HERE ---
            stack="Total",  # 1. This tells pyecharts to stack the series
            areastyle_opts=opts.AreaStyleOpts(
                opacity=0.5
            ),  # 2. This fills the area under the line
            is_symbol_show=False,
        )

    return line


# Function to generate a line chart with multiple lines
def plot_lines_chart(
    x_data: np.ndarray,
    ys_data: List[np.ndarray],
    names: List[str],
    range_start: int = 0,
    range_end: int = 100,
    lower_bound: float = None,
    up_bound: float = None,
):
    assert len(ys_data) == len(names), "Length of ys_data and names should be the same"
    line = Line(
        init_opts={
            "width": "1560px",
            "height": "600px",
            "is_horizontal_center": True,
        }
    ).add_xaxis(list(x_data))
    for i, y_data in enumerate(ys_data):
        line.add_yaxis(names[i], list(y_data), is_symbol_show=False)

    line.set_global_opts(
        xaxis_opts=opts.AxisOpts(type_="category"),
        yaxis_opts=opts.AxisOpts(type_="value", min_=lower_bound, max_=up_bound),
        legend_opts=opts.LegendOpts(
            textstyle_opts=opts.TextStyleOpts(font_weight="bold", font_size=17),
        ),
        datazoom_opts=[
            opts.DataZoomOpts(
                range_start=range_start, range_end=range_end, orient="horizontal"
            )
        ],
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
    ).set_series_opts(
        linestyle_opts=opts.LineStyleOpts(width=3),
    )
    return line


# 两个y轴的折线图, 分别使用左右y轴不同的刻度
def plot_dual_y_line_chart(
    x_data: np.ndarray,
    ys_data: list[np.ndarray],
    names: List[str],
    range_start: int = 0,
    range_end: int = 100,
):
    assert (
        len(ys_data) == len(names) == 2
    ), "Length of ys_data and names should be the same"
    y1_data, y2_data = ys_data
    name1, name2 = names
    assert len(y1_data) == len(
        y2_data
    ), "Length of y1_data and y2_data should be the same"
    line = (
        Line(
            init_opts={
                "width": "1560px",
                "height": "600px",
                "is_horizontal_center": True,
            }
        )
        .add_xaxis(list(x_data))
        .add_yaxis(name1, list(y1_data), yaxis_index=0, is_symbol_show=False)
        .add_yaxis(name2, list(y2_data), yaxis_index=1, is_symbol_show=False)
        .extend_axis(
            yaxis=opts.AxisOpts(
                type_="value",
                axislabel_opts=opts.LabelOpts(is_show=True),
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=False),  # 网格线不显示
            )
        )
        .extend_axis(
            yaxis=opts.AxisOpts(
                type_="value",
                axislabel_opts=opts.LabelOpts(is_show=True),
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=False),  # 网格线不显示
            )
        )
        .set_global_opts(
            xaxis_opts=opts.AxisOpts(type_="category"),
            legend_opts=opts.LegendOpts(
                textstyle_opts=opts.TextStyleOpts(font_weight="bold", font_size=17)
            ),
            datazoom_opts=[
                opts.DataZoomOpts(
                    range_start=range_start, range_end=range_end, orient="horizontal"
                )
            ],
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
        )
        .set_series_opts(
            linestyle_opts=opts.LineStyleOpts(width=3),
        )
    )

    return line


def load_speed_of_indus(
    Sw_data_Folder: Path = Path(r"data/sw1"), end_date: np.datetime64 = None
):
    # 读取数据
    all_hist_sw1_df = pd.concat(
        [
            pd.read_csv(indus_i).assign(CODE=indus_i.stem)
            for indus_i in Sw_data_Folder.glob("*.csv")
        ],
        axis=0,
    )
    if end_date:
        all_hist_sw1_df["日期"] = pd.to_datetime(all_hist_sw1_df["日期"])
        all_hist_sw1_df = all_hist_sw1_df[all_hist_sw1_df["日期"] <= end_date]
        all_hist_sw1_df["日期"] = np.datetime_as_string(
            all_hist_sw1_df["日期"], unit="D"
        )

    all_hist_sw1_df["rtn"] = all_hist_sw1_df["PCT_CHG"] / 100
    all_hist_sw1_df["rank_of_rtn"] = all_hist_sw1_df.groupby("日期")["rtn"].rank(
        ascending=True, pct=True
    )
    all_hist_sw1_df["std_of_rankMonthlyRtn"] = (
        all_hist_sw1_df.groupby("CODE")["rank_of_rtn"]
        .rolling(window=20)
        .std()
        .reset_index(0, drop=True)
        .values
    )
    all_hist_sw1_df["std_of_rankWeeklyRtn"] = (
        all_hist_sw1_df.groupby("CODE")["rank_of_rtn"]
        .rolling(window=5)
        .std()
        .reset_index(0, drop=True)
        .values
    )
    all_hist_sw1_df.dropna(subset=["std_of_rankMonthlyRtn"], inplace=True)
    speed_of_idus_monthly = (
        all_hist_sw1_df.groupby("日期")["std_of_rankMonthlyRtn"].mean().to_frame()
    )
    speed_of_idus_weekly = (
        all_hist_sw1_df.groupby("日期")["std_of_rankWeeklyRtn"].mean().to_frame()
    )
    return speed_of_idus_monthly.round(3), speed_of_idus_weekly.round(3)


def load_speed_of_barra(end_date: np.datetime64 = None):
    engine = connect_to_database()
    cne5 = pd.read_sql_query("SELECT * FROM cne5", engine)
    cne5["日期"] = pd.to_datetime(cne5["日期"])
    if end_date:
        cne5 = cne5[cne5["日期"].dt.date <= pd.Timestamp(end_date).date()]
    cne5 = cne5.melt(id_vars=["日期"], var_name="barra", value_name="rtn").sort_values(
        ["日期", "barra"]
    )
    cne5["rank_of_rtn"] = cne5.groupby("日期")["rtn"].rank(ascending=True, pct=True)
    cne5["std_of_rankMonthlyRtn"] = (
        cne5.groupby("barra")["rank_of_rtn"]
        .rolling(window=20)
        .std()
        .reset_index(0, drop=True)
        .values
    )
    cne5["std_of_rankWeeklyRtn"] = (
        cne5.groupby("barra")["rank_of_rtn"]
        .rolling(window=5)
        .std()
        .reset_index(0, drop=True)
        .values
    )
    cne5.dropna(subset=["std_of_rankMonthlyRtn"], inplace=True)
    speed_of_barra_monthly = (
        cne5.groupby("日期")["std_of_rankMonthlyRtn"].mean().to_frame()
    )
    speed_of_barra_weekly = (
        cne5.groupby("日期")["std_of_rankWeeklyRtn"].mean().to_frame()
    )
    return speed_of_barra_monthly.round(3), speed_of_barra_weekly.round(3)
