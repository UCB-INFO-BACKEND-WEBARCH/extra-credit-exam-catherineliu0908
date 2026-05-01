import collections
import json
import os
import re

STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "if", "then", "of",
    "in", "on", "at", "to", "for", "with", "by", "is", "are",
    "was", "were", "be", "been", "being", "this", "that",
}


def _get_queue():
    import redis as redis_lib
    from rq import Queue
    return Queue(connection=redis_lib.from_url(os.environ["REDIS_URL"]))


def run_stage_1(job_id, text):
    from app import create_app
    app = create_app()
    with app.app_context():
        from models import db, Job
        job = Job.query.get(job_id)
        try:
            job.status = "running"
            job.current_stage = 1
            db.session.commit()

            os.makedirs(f"/data/{job_id}", exist_ok=True)
            with open(f"/data/{job_id}/stage1.txt", "w") as f:
                f.write(text)

            _get_queue().enqueue(run_stage_2, job_id)
        except Exception as e:
            job.status = "failed"
            job.failed_stage = 1
            job.error = str(e)
            db.session.commit()


def run_stage_2(job_id):
    from app import create_app
    app = create_app()
    with app.app_context():
        from models import db, Job
        job = Job.query.get(job_id)
        try:
            job.status = "running"
            job.current_stage = 2
            db.session.commit()

            with open(f"/data/{job_id}/stage1.txt", "r") as f:
                text = f.read()
            with open(f"/data/{job_id}/stage2.txt", "w") as f:
                f.write(text.lower())

            _get_queue().enqueue(run_stage_3, job_id)
        except Exception as e:
            job.status = "failed"
            job.failed_stage = 2
            job.error = str(e)
            db.session.commit()


def run_stage_3(job_id):
    from app import create_app
    app = create_app()
    with app.app_context():
        from models import db, Job
        job = Job.query.get(job_id)
        try:
            job.status = "running"
            job.current_stage = 3
            db.session.commit()

            with open(f"/data/{job_id}/stage2.txt", "r") as f:
                text = f.read()
            tokens = re.findall(r"[a-zA-Z]+", text)
            with open(f"/data/{job_id}/stage3.json", "w") as f:
                json.dump(tokens, f)

            _get_queue().enqueue(run_stage_4, job_id)
        except Exception as e:
            job.status = "failed"
            job.failed_stage = 3
            job.error = str(e)
            db.session.commit()


def run_stage_4(job_id):
    from app import create_app
    app = create_app()
    with app.app_context():
        from models import db, Job
        job = Job.query.get(job_id)
        try:
            job.status = "running"
            job.current_stage = 4
            db.session.commit()

            with open(f"/data/{job_id}/stage3.json", "r") as f:
                tokens = json.load(f)
            filtered = [t for t in tokens if t not in STOPWORDS]
            with open(f"/data/{job_id}/stage4.json", "w") as f:
                json.dump(filtered, f)

            _get_queue().enqueue(run_stage_5, job_id)
        except Exception as e:
            job.status = "failed"
            job.failed_stage = 4
            job.error = str(e)
            db.session.commit()


def run_stage_5(job_id):
    from app import create_app
    app = create_app()
    with app.app_context():
        from models import db, Job, TopWord
        job = Job.query.get(job_id)
        try:
            job.status = "running"
            job.current_stage = 5
            db.session.commit()

            with open(f"/data/{job_id}/stage4.json", "r") as f:
                tokens = json.load(f)
            if not tokens:
                raise ValueError("no tokens after stopword removal")

            counter = collections.Counter(tokens)
            with open(f"/data/{job_id}/stage5.json", "w") as f:
                json.dump(dict(counter), f)

            top_5 = counter.most_common(5)
            for word, count in top_5:
                db.session.add(TopWord(job_id=job_id, word=word, count=count))

            job.status = "completed"
            job.current_stage = 5
            db.session.commit()
        except Exception as e:
            job.status = "failed"
            job.failed_stage = 5
            job.error = str(e)
            db.session.commit()
