import os
import tempfile

import redis as redis_lib
from flask import Flask, jsonify
from sqlalchemy import text

from models import db

import uuid
from flask import request, abort
from rq import Queue


def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)

    with app.app_context():
        from models import Job, TopWord  # noqa: F401
        db.create_all()

    redis_conn = redis_lib.from_url(os.environ["REDIS_URL"])
    queue = Queue(connection=redis_conn)

    @app.route("/health")
    def health():
        db_status = "down"
        try:
            with app.app_context():
                db.session.execute(text("SELECT 1"))
            db_status = "up"
        except Exception:
            pass

        redis_status = "down"
        try:
            redis_lib.from_url(os.environ["REDIS_URL"]).ping()
            redis_status = "up"
        except Exception:
            pass

        volume_writable = False
        try:
            with tempfile.NamedTemporaryFile(dir="/data", delete=True):
                volume_writable = True
        except Exception:
            pass

        return jsonify({
            "status": "ok",
            "db": db_status,
            "redis": redis_status,
            "volume_writable": volume_writable,
        }), 200

    # TODO: register routes (POST /jobs, GET /jobs/<id>)
    
    @app.route("/jobs", methods=["POST"])
    def create_job():
        payload = request.get_json(silent=True) or {}
        text_input = payload.get("text")
        if text_input is None:
            return jsonify({"error": "missing 'text' field"}), 400

        from models import Job
        job_id = str(uuid.uuid4())
        job = Job(id=job_id, status="pending", current_stage=0)
        db.session.add(job)
        db.session.commit()

        from stages import run_stage_1
        queue.enqueue(run_stage_1, job_id, text_input)

        return jsonify({"job_id": job_id}), 202

    @app.route("/jobs/<job_id>", methods=["GET"])
    def get_job(job_id):
        from models import Job
        job = Job.query.get(job_id)
        if job is None:
            abort(404)
        return jsonify({
            "job_id": job.id,
            "status": job.status,
            "current_stage": job.current_stage,
            "failed_stage": job.failed_stage,
            "error": job.error,
        }), 200
    
    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
