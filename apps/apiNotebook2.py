import marimo

__generated_with = "0.10.7"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt
    import plotly_express as px
    import plotly.graph_objects as go
    return alt, go, mo, pl, px


@app.cell
def _(pl):
    data = pl.read_csv(
        r"C:\Users\ia4734\Desktop\Work\Projects\API response size threshold\Data\October file with response size.csv",
        infer_schema_length=1000000,
    ).with_columns((pl.col("MINResponseSize_KB") / 1024).alias("MB"),
                  
          pl.concat_str([pl.col('ProviderName'), pl.lit("-"), pl.col("ApiMethod")]).alias("ApiMethod")        
                  
                  )
    return (data,)


@app.cell
def _(data, mo):
    mo.md(
        f""" ## Total number of API methods : {data.select('ApiMethod').unique().count().item()}"""
    ).callout("success")
    return


@app.cell
def _(data):
    data.select("ApiMethod").unique().rename({'ApiMethod':'API Methods'})
    return


@app.cell
def _(mo):
    mo.md(r"""## -----------------------------------------------------------------------------------------------------------------------------------------------------------""")
    return


@app.cell
def _(data, mo, pl):
    mo.md(rf"""## Total number of Compliant API methods (100% compliance) : {data.group_by("ApiMethod").agg(
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
         pl.col("TotalHits").sum()
    ).select(((pl.col('RequestLessThan_750_MS_Based_On_SuccessHits')/pl.col('TotalHits')).alias('Compliance'))).filter(pl.col('Compliance')==1).count().item()}""").callout(
        "success"
    )
    return


@app.cell
def _(data, pl):
    data.group_by(["ProviderName", "ApiMethod"]).agg(
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
    ).with_columns(
        (
            (
                pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
                / pl.col("TotalHits")
            ).alias("Compliance")
        )
    ).filter(pl.col("Compliance") == 1).sort("TotalHits", descending=True).rename(
        {
            "ProviderName": "ADGE",
            "ApiMethod": "API methods",
            "RequestLessThan_750_MS_Based_On_SuccessHits": "Compliant Hits",
            "TotalHits": "Total Hits",
        }
    )
    return


@app.cell
def _(data, mo, pl):
    slider = mo.ui.range_slider.from_series(
        data.group_by("ApiMethod")
        .agg(
            pl.col("MB").sum(),
        )
        .filter(pl.col("MB") >= 1)
        .select("MB")
        .to_series()
    )
    return (slider,)


@app.cell
def _(mo):
    mo.md(r"""## -----------------------------------------------------------------------------------------------------------------------------------------------------""")
    return


@app.cell
def _(data, mo, pl, slider):
    mo.md(rf"""
    # API Methods Size Analysis

    ## {slider} 

    ## API methods >= {slider.value[0]} MB : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).select('ApiMethod').count().item()}

    ##     API methods <= {slider.value[1]} MB : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
    ).filter((pl.col("MB")<=slider.value[0])|(pl.col("MB")>=slider.value[1])).select('ApiMethod').count().item()}

    """).callout("success")
    return


@app.cell
def _(data, pl, px, slider):
    px.pie(
        data.with_columns(
            (
                pl.when(pl.col("MB").is_between(slider.value[0], slider.value[1]))
                .then(pl.lit(f"More Than {slider.value[0]} MB"))
                .otherwise(pl.lit(f"Less than {slider.value[1]} MB"))
            ).alias("Size")
        ),
        names="Size",
        title="% of MB Size",
    )
    return


@app.cell
def _(data, mo, pl, slider):
    mo.md(f"""

    ## Median Size : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('MB').median().item()}

    ## Count of 100 % Compliance : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).filter(pl.col('Compliance')==1).select("Compliance").count().item()}

    ## Median Compliance % : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('Compliance').median().item()}

    ## Avg Compliance % : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('Compliance').mean().item()}

    ## Max Compliance % : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('Compliance').max().item()}

    ## Min Compliance % : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('Compliance').min().item()}

    ## Median Response Time : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('MedianResTime_Sec').median().item()}

    ## Avg Response Time : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('MedianResTime_Sec').mean().item()}

    ## Max Response Time : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('MedianResTime_Sec').max().item()}

    ## Min Response Time : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('MedianResTime_Sec').min().item()}

    ## Total Hits : {data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col('MedianResTime_Sec').sum()
    ).filter(pl.col("MB").is_between(slider.value[0], slider.value[1])).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).select('TotalHits').sum().item()}




    """)
    return


@app.cell
def _(data, pl, slider):
    data.group_by("ApiMethod").agg(
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
        pl.col("MedianResTime_Sec").sum(),
    ).filter(
        pl.col("MB").is_between(slider.value[0], slider.value[1])
    ).with_columns(
        (pl.col("TotalHits") / pl.col("TotalHits").sum()).alias("Weights"),
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).sort("MB", descending=True).rename(
        {
            "ApiMethod": "API methods",
            "RequestLessThan_750_MS_Based_On_SuccessHits": "Compliant Hits",
            "TotalHits": "Total Hits",'MedianResTime_Sec': 'Meduan Response Time in Sec'
        }
    )
    return


@app.cell
def _(mo):
    mo.md(r"""### Impact on Entities""")
    return


@app.cell
def _(data, pl, slider):
    data.group_by("ProviderName").agg(
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
    ).select(
        "ProviderName",
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ).join(
    data.group_by("ApiMethod").agg(
        pl.col("ProviderName").first(),
        pl.col("MB").sum(),
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
    ).filter((pl.col("MB")<=slider.value[0])|(pl.col("MB")>=slider.value[1])).group_by("ProviderName").agg(
        pl.col("RequestLessThan_750_MS_Based_On_SuccessHits").sum(),
        pl.col("TotalHits").sum(),
    ).select(
        "ProviderName",
        (
            pl.col("RequestLessThan_750_MS_Based_On_SuccessHits")
            / pl.col("TotalHits")
        ).alias("Compliance"),
    ),
        on = 'ProviderName' , how = 'full'

        
    ).drop('ProviderName_right').with_columns((pl.col('Compliance_right')-pl.col('Compliance')).alias('Growth')).rename({"Compliance" : "Compliance Before", "Compliance_right" : "Compliance After"}).sort('Growth', descending = True)
    return


@app.cell
def _(mo):
    mo.md('''# Size vs Response Time (Sec)''').callout('success')
    return


@app.cell
def _(data, pl, px):
    px.scatter(
        data.group_by("ApiMethod").agg(
            pl.col("MB").sum(), pl.col("MedianResTime_Sec").sum()
        ).rename({'MedianResTime_Sec' : 'Response Time in Sec'}),
        x="MB",
        y="Response Time in Sec",
    )
    return


@app.cell
def _(mo):
    mo.md("""# Size by Buckets""").callout('success')
    return


@app.cell
def _(data, pl, px):
    px.bar(
        data.sort("MB").with_columns(
        pl.col("MB")
        .cut(
            [0, 1, 5, 10, 20, 50, 100, 150, 200],
            labels=[
                "0",
                "0-1",
                "1-5",
                "5-10",
                "10-20",
                "20-50",
                "50-100",
                "100-150",
                "150-200",
                "200-more",
            ],
        )
        .alias("Bucket")
    ).group_by("Bucket").agg(
        pl.col("ApiMethod").n_unique(), pl.col("MedianResTime_Sec").median()
    ),
        x="Bucket",
        y="ApiMethod",
        labels={"Api MB buckets": "API MB Buckets", "Count of API methods": "Count of API Methods"},
        title="Count and Response Time by Buckets", color='MedianResTime_Sec'
    )
    return


@app.cell
def _(data, pl):
    data.sort("MB").with_columns(
        pl.col("MB")
        .cut(
            [0, 1, 5, 10, 20, 50, 100, 150, 200],
            labels=[
                "0",
                "0-1",
                "1-5",
                "5-10",
                "10-20",
                "20-50",
                "50-100",
                "100-150",
                "150-200",
                "200-more",
            ],
        )
        .alias("Bucket")
    ).group_by("Bucket").agg(
        pl.col("ApiMethod").n_unique(), pl.col("MedianResTime_Sec").median()
    )
    return


if __name__ == "__main__":
    app.run()
