# Databricks notebook source
# MAGIC %md
# MAGIC # IPL Data Analysis using PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ball vs Ball Schema

# COMMAND ----------

from pyspark.sql.functions import col,when,sum,avg,row_number
from pyspark.sql.types import *
from pyspark.sql import Window

# COMMAND ----------

# Define the schema
ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])


# COMMAND ----------

df_ball = spark.read.schema(ball_schema).format("csv").option("header","true").load("dbfs:/mnt/dbxcont/Ball_By_Ball.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match Schema

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),  # PySpark doesn't have a specific "year" type, use IntegerType
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

df_match = spark.read.schema(match_schema).format("csv").option("header","true").load("dbfs:/mnt/dbxcont/Match.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Player Schema

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

df_player = spark.read.schema(player_schema).format("csv").option("header","true").load("dbfs:/mnt/dbxcont/Player.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player Match Schema

# COMMAND ----------

# Define the schema
pl_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),  # PySpark uses IntegerType for year
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

df_player_match = spark.read.schema(pl_match_schema).format("csv").option("header","true").load("dbfs:/mnt/dbxcont/Player_match.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Team schema

# COMMAND ----------

# Define the schema
team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

df_team = spark.read.schema(team_schema).format("csv").option("header","true").load("dbfs:/mnt/dbxcont/Team.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

df_ball = df_ball.filter((col("wides") == 0) & (col("noballs") == 0))
display(df_ball)

# COMMAND ----------

# total and avg runs scored in each match match and innings

total_avg_runs = df_ball.groupBy("match_id","innings_no").agg(
  sum("runs_scored").alias("total_runs"),
  avg(col("runs_scored")).alias("avg_runs")
)

display(total_avg_runs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Windows function

# COMMAND ----------

# windows function

windowsSpec= Window.partitionBy("match_id","innings_no").orderBy("over_id")

df_ball = df_ball.withColumn("total_runs",
                             sum("runs_scored").over(windowsSpec))

df_ball = df_ball.withColumn("avg_runs",
                             avg("runs_scored").over(windowsSpec))

display(df_ball)

# COMMAND ----------

# Conditional column: Flag for high impact balls (either wicket or more than 6 runs including extras)

df_ball = df_ball.withColumn("high_impact",
    when((col("runs_scored") + col("extra_runs") > 6) | (col("bowler_wicket") == True), True).otherwise(False))

df_ball.show()

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, when

# Extracting year, month, and day from the match date for more detailed time-based analysis
df_match = df_match.withColumn("year", year("match_date"))
df_match = df_match.withColumn("month", month("match_date"))
df_match = df_match.withColumn("day", dayofmonth("match_date"))

# High margin win: categorizing win margins into 'high', 'medium', and 'low'
df_match = df_match.withColumn(
    "win_margin_category",
    when(col("win_margin") >= 100, "High")
    .when((col("win_margin") >= 50) & (col("win_margin") < 100), "Medium")
    .otherwise("Low")
)

# Analyze the impact of the toss: who wins the toss and the match
df_match = df_match.withColumn(
    "toss_match_winner",
    when(col("toss_winner") == col("match_winner"), "Yes").otherwise("No")
)

# Show the enhanced match DataFrame
display(df_match)

# COMMAND ----------

from pyspark.sql.functions import lower, regexp_replace

# Normalize and clean player names
df_player = df_player.withColumn("player_name", lower(regexp_replace("player_name", "[^a-zA-Z0-9 ]", "")))

# Handle missing values in 'batting_hand' and 'bowling_skill' with a default 'unknown'
df_player = df_player.na.fill({"batting_hand": "unknown", "bowling_skill": "unknown"})

# Categorizing players based on batting hand
df_player= df_player.withColumn(
    "batting_style",
    when(col("batting_hand").contains("left"), "Left-Handed").otherwise("Right-Handed")
)

# Show the modified player DataFrame
df_player.show(2)


# COMMAND ----------


from pyspark.sql.functions import col, when, current_date, expr

# Add a 'veteran_status' column based on player age
df_player_match = df_player_match.withColumn(
    "veteran_status",
    when(col("age_as_on_match") >= 35, "Veteran").otherwise("Non-Veteran")
)

# Dynamic column to calculate years since debut
df_player_match = df_player_match.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)

# Show the enriched DataFrame
df_player_match.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Temp views for quering using Spark SQL

# COMMAND ----------

df_ball.createOrReplaceTempView("ball_by_ball")
df_match.createOrReplaceTempView("match")
df_player.createOrReplaceTempView("player")
df_player_match.createOrReplaceTempView("player_match")
df_team.createOrReplaceTempView("team")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations using SQL (Can use Spark SQL as well)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top scoring batsman in each season
# MAGIC
# MAGIC WITH ranked_batsmen AS (
# MAGIC     SELECT 
# MAGIC         p.player_name,
# MAGIC         m.season_year,
# MAGIC         SUM(b.runs_scored) AS total_runs,
# MAGIC         RANK() OVER (PARTITION BY m.season_year ORDER BY SUM(b.runs_scored) DESC) AS rank
# MAGIC     FROM ball_by_ball b
# MAGIC     JOIN match m ON b.match_id = m.match_id   
# MAGIC     JOIN player_match pm ON m.match_id = pm.match_id AND b.striker = pm.player_id     
# MAGIC     JOIN player p ON p.player_id = pm.player_id
# MAGIC     GROUP BY p.player_name, m.season_year
# MAGIC )
# MAGIC SELECT 
# MAGIC     player_name,
# MAGIC     season_year,
# MAGIC     total_runs
# MAGIC FROM ranked_batsmen
# MAGIC WHERE rank = 1
# MAGIC ORDER BY season_year;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- economical_bowlers_powerplay
# MAGIC SELECT 
# MAGIC p.player_name, 
# MAGIC AVG(b.runs_scored) AS avg_runs_per_ball, 
# MAGIC COUNT(b.bowler_wicket) AS total_wickets
# MAGIC FROM ball_by_ball b
# MAGIC JOIN player_match pm ON b.match_id = pm.match_id AND b.bowler = pm.player_id
# MAGIC JOIN player p ON pm.player_id = p.player_id
# MAGIC WHERE b.over_id <= 6
# MAGIC GROUP BY p.player_name
# MAGIC HAVING COUNT(*) >= 1
# MAGIC ORDER BY avg_runs_per_ball, total_wickets DESC
# MAGIC LIMIT 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- toss_impact_individual_matches 
# MAGIC
# MAGIC SELECT m.match_id, m.toss_winner, m.toss_name, m.match_winner,
# MAGIC        CASE WHEN m.toss_winner = m.match_winner THEN 'Won' ELSE 'Lost' END AS match_outcome
# MAGIC FROM match m
# MAGIC WHERE m.toss_name IS NOT NULL
# MAGIC ORDER BY m.match_id
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT venue_name, AVG(total_runs) AS average_score, MAX(total_runs) AS highest_score
# MAGIC FROM (
# MAGIC     SELECT ball_by_ball.match_id, match.venue_name, SUM(runs_scored) AS total_runs
# MAGIC     FROM ball_by_ball
# MAGIC     JOIN match ON ball_by_ball.match_id = match.match_id
# MAGIC     GROUP BY ball_by_ball.match_id, match.venue_name
# MAGIC )
# MAGIC GROUP BY venue_name
# MAGIC ORDER BY average_score DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT out_type, COUNT(*) AS frequency
# MAGIC FROM ball_by_ball
# MAGIC WHERE out_type IS NOT NULL
# MAGIC GROUP BY out_type
# MAGIC ORDER BY frequency DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team1, COUNT(*) AS matches_played, SUM(CASE WHEN toss_winner = match_winner THEN 1 ELSE 0 END) AS wins_after_toss
# MAGIC FROM match
# MAGIC WHERE toss_winner = team1
# MAGIC GROUP BY team1
# MAGIC ORDER BY wins_after_toss DESC