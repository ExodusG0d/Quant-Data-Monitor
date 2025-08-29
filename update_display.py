import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from utils import (
    connect_to_database,
    load_hist_data_from_wind,
    load_bench_cons,
    calculate_percentile,
    plot_line_chart,
    plot_dual_y_line_chart,
    plot_lines_chart,
    plot_stacked_area_chart,
    plot_100_percent_stacked_area_chart,
    load_speed_of_indus,
    load_speed_of_barra,
)
from window import rolling_mean, rolling_std
from typing import List
from jinja2 import Environment, FileSystemLoader


END_DATE = np.datetime64(datetime.now())
pat = -6
# pat = -3

class Specify_dict(dict):
    def update(self, data: dict):
        for key, value in data.items():
            eixt_value: List = self.get(key)
            eixt_value.append(value)
            self[key] = eixt_value


display_dict = {}

# Main execution and plotting
if __name__ == "__main__":
    combined_fig = Specify_dict(
        {
            "📊 Market Overview": [],
            "base": [],
            "中证全指": [],
            "沪深300": [],
            "中证500": [],
            "中证1000": [],
            "中证2000": [],
        }
    )
    engine = connect_to_database()

    # barra 风格因子
    query_cne5 = "SELECT * FROM cne5"
    cne5 = pd.read_sql_query(query_cne5, engine).iloc[-20:]
    cne5["日期"] = pd.to_datetime(cne5["日期"])
    if END_DATE:
        cne5 = cne5[cne5["日期"] <= END_DATE]
    cne5["日期"] = np.datetime_as_string(cne5["日期"], unit="D")
    cne5.set_index("日期", inplace=True)
    cne5.iloc[0, :] = 0
    cne5_nav = (1 + cne5).cumprod()
    combined_fig.update(
        {
            "base": plot_lines_chart(
                x_data=cne5_nav.index,
                ys_data=[cne5_nav[col].values.round(3) for col in cne5_nav.columns],
                names=[col for col in cne5_nav.columns],
                lower_bound=np.round(min(cne5_nav.min().values) - 0.02, 2),
            )
        }
    )

    # Plot 指数成交金额(绘制在一幅图中)
    all_mean5_volumeRMB = []
    all_daily_volumeRMB = []
    query_bench = f"SELECT * FROM bench_basic_data"
    hist_all_bench_df = pd.read_sql_query(query_bench, engine)
    print("Plot 指数成交金额(合体图)")
    for bench, name in {
        "000985.CSI": "中证全指",
        "000300.SH": "沪深300",
        "000905.SH": "中证500",
        "000852.SH": "中证1000",
        "932000.CSI": "中证2000",
    }.items():
        hist_bench_df = hist_all_bench_df[hist_all_bench_df['code']==bench].copy().sort_values(by="date")
        if END_DATE:
            hist_bench_df["date"] = pd.to_datetime(hist_bench_df["date"], errors="coerce")
            hist_bench_df = hist_bench_df[hist_bench_df["date"] <= END_DATE]
            hist_bench_df["date"] = np.datetime_as_string(
                hist_bench_df["date"], unit="D"
            )
        volumeRMB = hist_bench_df["AMT"].values / 1e8
        daily_volumeRMB = volumeRMB.round(2)[-250:]
        weekly_mean_volumeRMB = rolling_mean(volumeRMB, 5).round(2)[-250:]
        all_mean5_volumeRMB.append(weekly_mean_volumeRMB)
        if name != "中证全指": #这是后面绘制成交占比，所以不能要全指的
            all_daily_volumeRMB.append(daily_volumeRMB)
        display_dict.update(
            {
                f"{name}成交金额MA5": [
                    weekly_mean_volumeRMB[-1],
                    weekly_mean_volumeRMB[pat],
                ],
                f"{name}成交金额": [
                    daily_volumeRMB[-1],
                    daily_volumeRMB[pat],
                ]
            }
        )

    combined_fig.update(
        {
            "base": plot_lines_chart(
                x_data=hist_bench_df["date"].values[-250:],
                ys_data=all_mean5_volumeRMB,
                names=[
                    f"{name}成交金额MA5"
                    for name in [
                        "中证全指",
                        "沪深300",
                        "中证500",
                        "中证1000",
                        "中证2000",
                    ]
                ],
                range_start=75,
            )
        }
    )

    combined_fig.update(
        {
            "base": plot_stacked_area_chart(
                x_data=hist_bench_df["date"].values[-250:],
                ys_data=all_daily_volumeRMB,
                names=[
                    f"{name}"
                    for name in [
                        "沪深300",
                        "中证500",
                        "中证1000",
                        "中证2000",
                    ]
                ],
                title="各指数成交金额堆积"
            )
        }
    )

    combined_fig.update(
        {
            "base": plot_100_percent_stacked_area_chart(
                x_data=hist_bench_df["date"].values[-250:],
                ys_data=all_daily_volumeRMB,
                names=[
                    f"{name}"
                    for name in [
                        "沪深300",
                        "中证500",
                        "中证1000",
                        "中证2000",
                    ]
                ],
                title="各指数成交金额占比"
            )
        }
    )

    # 各自绘图的大循环

    for bench, name in {
        "000985.CSI": "中证全指",
        "000300.SH": "沪深300",
        "000905.SH": "中证500",
        "000852.SH": "中证1000",
        "932000.CSI": "中证2000",
    }.items():

        # Plot 指数成交金额（各自绘图，并包含分位数信息）
        print("Plot 指数成交金额(各自图)")
        hist_bench_df = hist_all_bench_df[hist_all_bench_df['code']==bench].copy().sort_values(by="date")
        if END_DATE:
            hist_bench_df["date"] = pd.to_datetime(hist_bench_df["date"], errors='coerce')
            hist_bench_df = hist_bench_df[hist_bench_df["date"] <= END_DATE]
            hist_bench_df["date"] = np.datetime_as_string(
                hist_bench_df["date"], unit="D"
            )
        # Calculate and plot 250-day rolling 成交金额 percentile
        VolumeRMB = hist_bench_df["AMT"].values / 1e8
        percentile = calculate_percentile(VolumeRMB, 250)
        combined_fig.update(
            {
                name: plot_dual_y_line_chart(
                    x_data=hist_bench_df["date"].values[-250:],
                    ys_data=[VolumeRMB[-250:], percentile[-250:]],
                    names=[f"{name}成交金额", f"{name}成交金额分位数"],
                    range_start=75,
                )
            }
        )

        # Plot 指数20日滚动年化波动率
        print("Plot 指数20日滚动年化波动率")
        # calculation
        hist_bench_df = hist_all_bench_df[hist_all_bench_df["code"] == bench].copy().sort_values(by="date")
        values_array = hist_bench_df['PCT_CHG'].values
        rolling_volatility = rolling_std(values_array, 20) * np.sqrt(252)
        weekly_mean_rolling_volatility = rolling_mean(rolling_volatility, 5).round(3)
        # plot
        combined_fig.update(
            {
                name: plot_lines_chart(
                    x_data=hist_bench_df["date"].values[-250:],
                    ys_data=[rolling_volatility[-250:], weekly_mean_rolling_volatility[-250:]],
                    names=[f"{name}20日滚动年化波动率", f"{name}20日滚动年化波动率MA5"],
                    range_start=75,
                )
            }
        )

        # # 计算各自成分股波动率
        # rtn = load_hist_data_from_wind(
        #     indicator="PCT_CHG",
        #     symols=load_bench_cons(bench),
        #     end_date=END_DATE if END_DATE else np.datetime64("today"),
        # )
        # rtn /= 100
        # vol = rtn.std(axis=1).to_frame()
        # percentile = calculate_percentile(vol[0].values, 250)
        # weekly_mean_percentile = rolling_mean(percentile, 5).round(3)
        # display_dict.update(
        #     {
        #         f"{name}波动率分位数MA5": [
        #             weekly_mean_percentile[-1],
        #             weekly_mean_percentile[pat],
        #         ]
        #     }
        # )
        # combined_fig.update(
        #     {
        #         name: plot_lines_chart(
        #             x_data=vol.index.strftime("%Y-%m-%d")[249:],
        #             ys_data=[
        #                 percentile,
        #                 weekly_mean_percentile.round(3),
        #             ],
        #             names=[
        #                 f"{name}成分股波动率分位数",
        #                 f"{name}成分股波动率分位数MA5",
        #             ],
        #             range_start=75,
        #         )
        #     }
        # )

    #     # 赚钱效应
    #     # NOTE: bench rtn 只会有前一天的数据, 而rtn中会有当天的数据, 故需要剔除最后一天的数据
    #     bench_rtn = hist_bench_df[["日期", "PCT_CHG"]].copy()
    #     bench_rtn["PCT_CHG"] = bench_rtn["PCT_CHG"] / 100
    #     bench_rtn = bench_rtn.set_index("日期").reindex(
    #         index=rtn.index.strftime("%Y-%m-%d")
    #     )
    #     win_ratio = ((rtn.values[-250:, :] - bench_rtn.values[-250:]) > 0).mean(axis=1)
    #     combined_fig.update(
    #         {
    #             name: plot_lines_chart(
    #                 x_data=bench_rtn.index.values[-250:],
    #                 ys_data=[win_ratio.round(3), rolling_mean(win_ratio, 5).round(3)],
    #                 names=[f"{name}赚钱效应", f"{name}赚钱效应MA5"],
    #                 range_start=75,
    #             )
    #         }
    #     )

    # # Plot 大小盘相对强弱
    # big_df = pd.read_csv(Path(r"data/801811.SI.csv"))
    # if END_DATE:
    #     big_df["日期"] = pd.to_datetime(big_df["日期"])
    #     big_df = big_df[big_df["日期"] <= END_DATE]
    #     big_df["日期"] = np.datetime_as_string(big_df["日期"], unit="D")
    # small_df = pd.read_csv(Path(r"data/801813.SI.csv"))
    # if END_DATE:
    #     small_df["日期"] = pd.to_datetime(small_df["日期"])
    #     small_df = small_df[small_df["日期"] <= END_DATE]
    #     small_df["日期"] = np.datetime_as_string(small_df["日期"], unit="D")

    # big_df["rtn"] = big_df["PCT_CHG"].values / 100
    # small_df["rtn"] = small_df["PCT_CHG"].values / 100
    # big_vs_small = big_df["rtn"] - small_df["rtn"]
    # combined_fig.update(
    #     {
    #         "base": plot_line_chart(
    #             x_data=big_df["日期"].values[-100:],
    #             y_data=big_vs_small.values[-100:].round(3),
    #             name="大小盘相对强弱",
    #             range_start=75,
    #         )
    #     }
    # )
    # display_dict.update(
    #     {
    #         "大小盘相对强弱": [
    #             big_vs_small.values[-1],
    #             big_vs_small.values[pat],
    #         ]
    #     }
    # )

    # Plot 价值VS成长相对强弱
    print("Plot 价值VS成长相对强弱")
    cni_399370 = hist_all_bench_df[hist_all_bench_df["code"] == "399370.SZ"].copy().sort_values(by="date").reset_index(drop=True)
    cni_399371 = hist_all_bench_df[hist_all_bench_df["code"] == "399371.SZ"].copy().sort_values(by="date").reset_index(drop=True) # drop the indexes to ensure the alignment
    if END_DATE:
        cni_399371["date"] = pd.to_datetime(cni_399371["date"])
        cni_399371 = cni_399371[cni_399371["date"] <= END_DATE]
        cni_399371["date"] = np.datetime_as_string(cni_399371["date"], unit="D")
    if END_DATE:
        cni_399370["date"] = pd.to_datetime(cni_399370["date"])
        cni_399370 = cni_399370[cni_399370["date"] <= END_DATE]
        cni_399370["date"] = np.datetime_as_string(cni_399370["date"], unit="D")
    value_vs_growth = (cni_399371["PCT_CHG"] - cni_399370["PCT_CHG"]) / 100
    combined_fig.update(
        {
            "base": plot_line_chart(
                x_data=cni_399370["date"].values[-100:],
                y_data=value_vs_growth.values[-100:].round(3),
                name="价值VS成长相对强弱",
                range_start=75,
            )
        }
    )
    display_dict.update(
        {
            "价值VS成长相对强弱": [
                value_vs_growth.values[-1],
                value_vs_growth.values[pat],
            ]
        }
    )

    # # 行业轮动
    # speed_of_idus_monthly, speed_of_idus_weekly = load_speed_of_indus(
    #     Path(r"data/sw1"), end_date=END_DATE
    # )
    # combined_fig.update(
    #     {
    #         "base": plot_lines_chart(
    #             x_data=speed_of_idus_monthly.index[-100:],
    #             ys_data=[
    #                 speed_of_idus_monthly.iloc[-100:, 0].values,
    #                 speed_of_idus_weekly.iloc[-100:, 0].values,
    #             ],
    #             names=["行业轮动速度(月)", "行业轮动速度(周)"],
    #             range_start=75,
    #             lower_bound=min(
    #                 min(speed_of_idus_monthly.iloc[-100:, 0].values),
    #                 min(speed_of_idus_weekly.iloc[-100:, 0].values),
    #             )
    #             - 0.02,
    #         )
    #     }
    # )
    # display_dict.update(
    #     {
    #         "行业轮动速度(月)": [
    #             speed_of_idus_monthly.iloc[-1, 0],
    #             speed_of_idus_monthly.iloc[pat, 0],
    #         ],
    #         "行业轮动速度(周)": [
    #             speed_of_idus_weekly.iloc[-1, 0],
    #             speed_of_idus_weekly.iloc[pat, 0],
    #         ],
    #     }
    # )

    #### base部分图 ####

    # barra 轮动速度
    speed_of_barra_monthly, speed_of_barra_weekly = load_speed_of_barra(
        end_date=END_DATE
    )
    print("Plot Barra 轮动速度")
    combined_fig.update(
        {
            "base": plot_lines_chart(
                x_data=np.datetime_as_string(
                    speed_of_barra_monthly.index[-100:], unit="D"
                ),
                ys_data=[
                    speed_of_barra_monthly.iloc[-100:, 0].values,
                    speed_of_barra_weekly.iloc[-100:, 0].values,
                ],
                names=["Barra轮动速度(月)", "Barra轮动速度(周)"],
                range_start=75,
                lower_bound=min(
                    min(speed_of_barra_monthly.iloc[-100:, 0].values.round(2)),
                    min(speed_of_barra_weekly.iloc[-100:, 0].values.round(2)),
                )
                - 0.02,
            )
        }
    )
    display_dict.update(
        {
            "Barra轮动速度(月)": [
                speed_of_barra_monthly.iloc[-1, 0],
                speed_of_barra_monthly.iloc[pat, 0],
            ],
            "Barra轮动速度(周)": [
                speed_of_barra_weekly.iloc[-1, 0],
                speed_of_barra_weekly.iloc[pat, 0],
            ],
        }
    )

    # Plot IC and IM data with dual Y-axis
    engine_futures = connect_to_database()
    IF_query = "SELECT * FROM IF_data"
    IC_query = "SELECT * FROM IC_data"
    IM_query = "SELECT * FROM IM_data"
    IF_data = pd.read_sql_query(IF_query, engine_futures)
    if END_DATE:
        IF_data["日期"] = pd.to_datetime(IF_data["日期"])
        IF_data = IF_data[IF_data["日期"] <= END_DATE]
        IF_data["日期"] = np.datetime_as_string(IF_data["日期"], unit="D")
    IC_data = pd.read_sql_query(IC_query, engine_futures)
    if END_DATE:
        IC_data["日期"] = pd.to_datetime(IC_data["日期"])
        IC_data = IC_data[IC_data["日期"] <= END_DATE]
        IC_data["日期"] = np.datetime_as_string(IC_data["日期"], unit="D")
    IM_data = pd.read_sql_query(IM_query, engine_futures)
    if END_DATE:
        IM_data["日期"] = pd.to_datetime(IM_data["日期"])
        IM_data = IM_data[IM_data["日期"] <= END_DATE]
        IM_data["日期"] = np.datetime_as_string(IM_data["日期"], unit="D")

    print("Plot 期指基差")
    combined_fig.update(
        {
            "base": plot_lines_chart(
                x_data=IC_data["日期"],
                ys_data=[
                    IF_data["年化基差(%)"],
                    IC_data["年化基差(%)"],
                    IM_data["年化基差(%)"],
                ],
                names=["IF年化基差(%)", "IC年化基差(%)", "IM年化基差(%)"],
                range_start=75,
            )
        }
    )
    display_dict.update(
        {
            "IF年化基差(%)": [
                IF_data["年化基差(%)"].values[-1],
                IF_data["年化基差(%)"].values[pat],
            ],
            "IC年化基差(%)": [
                IC_data["年化基差(%)"].values[-1],
                IC_data["年化基差(%)"].values[pat],
            ],
            "IM年化基差(%)": [
                IM_data["年化基差(%)"].values[-1],
                IM_data["年化基差(%)"].values[pat],
            ],
        }
    )

    display_df = pd.DataFrame(display_dict, index=["当期", f"上期(T{pat+1})"]).T
    display_df["变化"] = display_df["当期"] - display_df[f"上期(T{pat+1})"]
    display_df["变化%"] = display_df["变化"] / display_df[f"上期(T{pat+1})"]
    display_df["变化%"] = display_df["变化%"].apply(lambda x: f"{x:.2%}")
    # 如果 "当期" 或者 f"上期(T{pat+1})" 为负数, 则 "变化%" 为 NULL
    display_df.loc[
        (display_df["当期"] < 0) | (display_df[f"上期(T{pat+1})"] < 0), "变化%"
    ] = np.nan

    ### Jinja2 Template Rendering ###

    env = Environment(loader=FileSystemLoader('.')) # Looks for templates in the current directory
    template = env.get_template('report_template_multipages.html')

    # 2. Organize all the pieces of data into a structured dictionary
    report_sections = {}
    for section_title, chart_objects in combined_fig.items():
        report_sections[section_title] = {
            "charts": [chart.render_embed() for chart in chart_objects],
            "summary_table": None,
            "barra_table": None,
        }

    if '📊 Market Overview' in report_sections:
        report_sections['📊 Market Overview']['summary_table'] = display_df.T.to_html(render_links=True)
        report_sections['📊 Market Overview']['barra_table'] = cne5.iloc[-5:].to_html()

    # 3. Render the template with your data
    html_content = template.render(
        report_title="Market Overall Monitor 📈",
        last_updated=datetime.now(ZoneInfo('Asia/Shanghai')).strftime("%Y-%m-%d %H:%M:%S"),
        end_date=np.datetime_as_string(END_DATE, unit="D"),
        report_sections=report_sections,
    )

    # 4. Write the final HTML to a file
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html_content)
