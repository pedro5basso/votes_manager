from streaming.jobs.new_votes_streaming_job import SparkJob

if __name__ == "__main__":
    job = SparkJob()
    job.run_job()
