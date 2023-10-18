from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("spark_lab_1").getOrCreate()

    github_data = spark.read.json("10K.github.jsonl")

    push_events = github_data.filter(github_data.type == 'PushEvent')

    push_events = push_events.select("actor.login", "payload.commits.message")

    push_events_data = push_events.collect()
    output_path = "result.csv"
    result_data = []
    n = 3
    for row in push_events_data:
        author_name = row["login"]
        commit_messages = row["message"]
        
        if commit_messages and isinstance(commit_messages, list):
            for commit_message in commit_messages:
                segments = [commit_message[i:i + n].lower() for i in range(0, len(commit_message), n) if len(commit_message[i:i + n].strip()) == 3]
                result = (author_name.lower(), *segments)
                
                print(result)
                result_data.append((author_name, ", ".join(segments)))
    result_df = spark.createDataFrame(result_data, ["Author", "3-Grams"])

    result_df.write.csv(output_path, header=True, mode="overwrite")
    spark.stop()


if __name__ == '__main__':
    main()
