import marimo

__generated_with = "0.10.7"
app = marimo.App(
    width="full",
    layout_file="layouts/apiNotebook.slides.json",
    auto_download=["html"],
)


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import statistics as st
    import pandas as pd
    return mo, pd, pl, st


@app.cell
def _(mo):
    mo.md(r"""# Source data (> 1 MB)""")
    return


@app.cell
def _(pl):
    source = (
        pl.read_csv(
            r"C:\Users\ia4734\Desktop\Work\Projects\API response size threshold\Data\Golden layer.csv"
        )
        .rename(
            {
                "Sum of MINResponseSize_KB": "ResponseSizeKB",
                "Sum of MedianResTime_Sec": "MedianResTimeSec",
                "Sum of RequestLessThan_750_MS_Based_On_SuccessHits": "CompliantHits",
                "Sum of TotalHits": "TotalHits",
            }
        )
        .drop(["weights", "compliance"])
    )

    pl.read_csv(
        r"C:\Users\ia4734\Desktop\Work\Projects\API response size threshold\Data\Golden layer.csv"
    ).rename(
        {
            "Sum of MINResponseSize_KB": "ResponseSizeKB",
            "Sum of MedianResTime_Sec": "MedianResTimeSec",
            "Sum of RequestLessThan_750_MS_Based_On_SuccessHits": "CompliantHits",
            "Sum of TotalHits": "TotalHits",
        }
    )
    return (source,)


@app.cell
def _(pl, source):
    source2= source.filter(pl.col('MB')>=2)
    return (source2,)


@app.cell
def _(pl, source):
    less = (
        pl.read_csv(
            r"C:\Users\ia4734\Desktop\Work\Projects\API response size threshold\Data\lessthan1mb.csv"
        )
        .rename(
            {
                "Sum of MINResponseSize_KB": "ResponseSizeKB",
                "Sum of MedianResTime_Sec": "MedianResTimeSec",
                "Sum of RequestLessThan_750_MS_Based_On_SuccessHits": "CompliantHits",
                "Sum of TotalHits": "TotalHits",
                "size>1mb": "more than 1 mb",
            }
        )
        .with_columns((pl.col("ResponseSizeKB") / 1024).alias("MB"))
        .select(
            [
                "ProviderName",
                "ApiName",
                "ResponseSizeKB",
                "MedianResTimeSec",
                "CompliantHits",
                "TotalHits",
                "more than 1 mb",
                "MB",
            ]
        )
    )

    alll = pl.concat(items=[source, less], how="vertical")
    return alll, less


@app.cell
def _(mo):
    mo.md(r"""## ratio of more and less""")
    return


@app.cell
def _(less, source):
    len(source) / len(source) + len(less)
    return


@app.cell
def _(mo):
    mo.md(r"""## Top 5 adges with high response sizes""")
    return


@app.cell
def _(pl, source):
    source.group_by("ProviderName").agg(
        pl.col("MB").sum(),
        pl.col("CompliantHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col("MedianResTimeSec").sum(),
    ).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weight"),
        (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance"),
    ).sort("MB", descending=True).select(
        ["ProviderName", "MB", "MedianResTimeSec", "Weight", "Compliance"]
    ).limit(5)
    return


@app.cell
def _(mo):
    mo.md(r"""## Count of ADGEs""")
    return


@app.cell
def _(pl, source):
    source.group_by("ProviderName").agg(
        pl.col("ProviderName").count().alias("count"),
        pl.col("MB").sum(),
        pl.col("CompliantHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col("MedianResTimeSec").sum(),
    ).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weight"),
        (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance"),
    ).sort("count", descending=True)
    return


@app.cell
def _(mo):
    mo.md(r"""## Top 5 APIs with high response sizes""")
    return


@app.cell
def _(pl, source):
    source.group_by("ApiName").agg(
        pl.col("ProviderName").first(),
        pl.col("MB").sum(),
        pl.col("CompliantHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col("MedianResTimeSec").sum(),
    ).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weight"),
        (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance"),
    ).sort("MB", descending=True).select(
        [
            "ApiName",
            "ProviderName",
            "MB",
            "MedianResTimeSec",
            "Weight",
            "Compliance",
        ]
    ).limit(5)
    return


@app.cell
def _(mo):
    mo.md(r"""## compliance with all""")
    return


@app.cell
def _(alll):
    alll.select(["CompliantHits"]).sum().item() / alll.select(
        ["TotalHits"]
    ).sum().item()
    return


@app.cell
def _(mo):
    mo.md(r"""## compliance without large docs""")
    return


@app.cell
def _(less):
    less.select(["CompliantHits"]).sum().item() / less.select(
        ["TotalHits"]
    ).sum().item()
    return


@app.cell
def _(mo):
    mo.md(r"""## compliance by adge for all""")
    return


@app.cell
def _(alll, pl):
    alll.group_by("ProviderName").agg(
        pl.col("CompliantHits").sum(), pl.col("TotalHits").sum()
    ).with_columns(
        (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance")
    ).sort("Compliance", descending=True)
    return


@app.cell
def _(mo):
    mo.md(r"""## compliance by adge for less""")
    return


@app.cell
def _(less, pl):
    less.group_by("ProviderName").agg(
        pl.col("CompliantHits").sum(), pl.col("TotalHits").sum()
    ).with_columns(
        (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance")
    ).sort("Compliance", descending=True)
    return


@app.cell
def _(mo):
    mo.md(r"""## Compliance change""")
    return


@app.cell
def _(alll, less, pl):
    alll.group_by("ProviderName").agg(
        pl.col("CompliantHits").sum(), pl.col("TotalHits").sum()
    ).with_columns(
        (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance")
    ).sort("Compliance", descending=True).select(
        ["ProviderName", "Compliance"]
    ).join(
        less.group_by("ProviderName")
        .agg(pl.col("CompliantHits").sum(), pl.col("TotalHits").sum())
        .with_columns(
            (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance")
        )
        .sort("Compliance", descending=True)
        .select(["ProviderName", "Compliance"]),
        how="full",
        on="ProviderName",
    ).drop(["ProviderName_right"]).rename(
        {"Compliance_right": "Compliance_After"}
    ).sort("Compliance_After", descending=True)
    return


@app.cell
def _(pl, source):
    source.group_by("ProviderName").agg(
        pl.col("MB").sum(),
        pl.col("CompliantHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col("MedianResTimeSec").sum(),
    ).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weight"),
        (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance"),
    ).sort(["MB","Weight","Compliance"], descending=[True,True,False]).select(
        [
            "ProviderName",
            "MB",
            "MedianResTimeSec",
            "Weight",
            "Compliance",
        ]
    ).filter(pl.col('Compliance')<0.9,pl.col('Weight')>=0.02).limit(5)
    return


@app.cell
def _(alll, pl):
    alll.group_by("ProviderName").agg(
        pl.col("CompliantHits").sum(), pl.col("TotalHits").sum()
    ).with_columns(
        (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance")
    ).drop(["CompliantHits", "TotalHits"])\
    .join(
        alll.filter(pl.col("MB") <= (pl.col("MB")).quantile(0.7,interpolation='linear')).group_by("ProviderName").agg(
        pl.col("CompliantHits").sum(), pl.col("TotalHits").sum()
    ).with_columns(
        (pl.col("CompliantHits") / pl.col("TotalHits")).alias("Compliance")
    ).drop(["CompliantHits", "TotalHits"]) , on = 'ProviderName' , how= 'full').drop('ProviderName_right').rename({'Compliance':"ComplianceWith2MB","Compliance_right":"ComplianceWithout2MB" }).with_columns((pl.col("ComplianceWithout2MB")-pl.col("ComplianceWith2MB")).alias("Growth")).sort("Growth",descending = True)
    return


@app.cell
def _(alll):
    alll.select("MB").quantile(quantile = 0.7, interpolation = 'linear')
    return


@app.cell
def _(st):
    def percentile(x : float , y : list):
        orderedList = sorted(y)
        firstMedian = st.median(orderedList)
        return firstMedian
    return (percentile,)


@app.cell
def _(percentile):
    percentile(50.0 , [9,2,8,3,7,4,6,5])
    return


@app.cell
def _():
    #NEEW----------------------------------------------------------------------------------------------------------------
    return


@app.cell
def _(pl):
    oct = pl.read_csv(r'C:\Users\ia4734\Desktop\Work\Projects\API response size threshold\Data\October file with response size.csv', infer_schema_length= 100000000)
    return (oct,)


@app.cell
def _():
    # compliance growth by adge 1mb
    return


@app.cell
def _(oct, pl):
    oct.group_by("ProviderName").agg(
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(), pl.col("TotalHits").sum()
    ).with_columns(
        (pl.col("RequestLessThan_750_MS_Based_On_SuccessHits") / pl.col("TotalHits")).alias("Compliance")
    ).drop(["RequestLessThan_750_MS_Based_On_SuccessHits", "TotalHits"])\
    .join(
        oct.filter(pl.col('MINResponseSize_KB')<= 1024*2).group_by("ProviderName").agg(
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(), pl.col("TotalHits").sum()
    ).with_columns(
        (pl.col("RequestLessThan_750_MS_Based_On_SuccessHits") / pl.col("TotalHits")).alias("Compliance")
    ).drop(["RequestLessThan_750_MS_Based_On_SuccessHits", "TotalHits"]) , on = 'ProviderName' , how= 'full').drop('ProviderName_right').rename({'Compliance':"ComplianceWith2MB","Compliance_right":"ComplianceWithout2MB" }).with_columns((pl.col("ComplianceWithout2MB")-pl.col("ComplianceWith2MB")).alias("Growth")).sort("Growth",descending = True)
    return


@app.cell
def _(oct, pl):
    oct.select(
        pl.lit("all"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum()
            / pl.col("TotalHits").sum()
        ).alias("ComplianceBefore"),
    ).join(oct.filter(pl.col('MINResponseSize_KB')<=1024*2).select(pl.lit('all'),(pl.col('RequestLessThan_750_MS_Based_On_SuccessHits').sum()/pl.col('TotalHits').sum()).alias('ComplianceAfter')),on='literal',how='full').drop(['literal','literal_right']).with_columns((pl.col('ComplianceAfter')-pl.col('ComplianceBefore')).alias('growth'))
    return


@app.cell
def _(oct, pl):
    oct.filter(pl.col('ProviderName')=='ADP').select(pl.col('ApiName')).unique()
    return


@app.cell
def _(oct, pl):
    oct.filter(pl.col('MINResponseSize_KB')>= 1024).select(pl.col('ApiMethod'),(pl.col('RequestLessThan_750_MS_Based_On_SuccessHits')/pl.col('TotalHits')).alias('Compliance'),pl.col('AvgResTime_Sec')).filter(pl.col('Compliance')==1).select(pl.col('AvgResTime_Sec').mean())

    return


if __name__ == "__main__":
    app.run()
