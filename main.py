from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("spark_lab_1").getOrCreate()

    github_data = spark.read.json("10K.github.jsonl")

    push_events = github_data.filter(github_data.type == 'PushEvent')

    push_events = push_events.select("actor.login", "payload.commits.message")

    push_events_data = push_events.collect()

    n = 3
    for row in push_events_data:
        author_name = row["login"]
        commit_messages = row["message"]
        
        if commit_messages and isinstance(commit_messages, list):
            for commit_message in commit_messages:
                ngrams = [commit_message[i:i + n] for i in range(len(commit_message) - n + 1)]

                print(f"Author: {author_name}")
                print(f"Commit Message: {commit_message}")
                print(f"3-Grams: {ngrams}")
                print("\n")
    spark.stop()


if __name__ == '__main__':
    main()
