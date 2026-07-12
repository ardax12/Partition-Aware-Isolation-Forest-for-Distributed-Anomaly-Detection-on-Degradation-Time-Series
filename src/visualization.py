import matplotlib.pyplot as plt
from pyspark.sql.functions import max

def plot_cycle_distribution(df):
    """
    Histogram of engine cycles.
    """

    pdf = df.select("cycle").toPandas()

    plt.figure(figsize=(8,5))
    plt.hist(pdf["cycle"], bins=30)

    plt.title("Cycle Distribution")
    plt.xlabel("Cycle")
    plt.ylabel("Count")

    plt.grid(True)

    plt.show()

def plot_rul_distribution(df):
    """
    Histogram of Remaining Useful Life.
    """

    pdf = df.select("RUL").toPandas()

    plt.figure(figsize=(8,5))
    plt.hist(pdf["RUL"], bins=30)

    plt.title("Remaining Useful Life Distribution")
    plt.xlabel("RUL")
    plt.ylabel("Count")

    plt.grid(True)

    plt.show()

def plot_engine_lifetimes(df):
    """
    Bar chart of engine lifetimes.
    """

    pdf = (
        df.groupBy("engine_id")
        .agg(max("cycle").alias("max_cycle"))
        .orderBy("engine_id")
        .toPandas()
    )

    plt.figure(figsize=(12,5))

    plt.bar(
        pdf["engine_id"],
        pdf["max_cycle"]
    )

    plt.title("Maximum Cycle Per Engine")
    plt.xlabel("Engine ID")
    plt.ylabel("Maximum Cycle")

    plt.show()

def plot_sensor(df, engine_id, sensor):

    pdf = (
        df.filter(df.engine_id == engine_id)
        .orderBy("cycle")
        .select("cycle", sensor)
        .toPandas()
    )

    plt.figure(figsize=(9,5))

    plt.plot(
        pdf["cycle"],
        pdf[sensor]
    )

    plt.title(f"{sensor} - Engine {engine_id}")

    plt.xlabel("Cycle")

    plt.ylabel(sensor)

    plt.grid(True)

    plt.show()

def plot_engine_rul(df, engine_id):

    pdf = (
        df.filter(df.engine_id == engine_id)
        .orderBy("cycle")
        .select("cycle", "RUL")
        .toPandas()
    )

    plt.figure(figsize=(9,5))

    plt.plot(
        pdf["cycle"],
        pdf["RUL"]
    )

    plt.title(f"Engine {engine_id} Remaining Useful Life")

    plt.xlabel("Cycle")

    plt.ylabel("RUL")

    plt.grid(True)

    plt.show()

def correlation_matrix(df):

    pdf = df.toPandas()

    corr = pdf.corr(numeric_only=True)

    plt.figure(figsize=(14,12))

    plt.imshow(corr)

    plt.colorbar()

    plt.xticks(
        range(len(corr.columns)),
        corr.columns,
        rotation=90
    )

    plt.yticks(
        range(len(corr.columns)),
        corr.columns
    )

    plt.title("Correlation Matrix")

    plt.tight_layout()

    plt.show()