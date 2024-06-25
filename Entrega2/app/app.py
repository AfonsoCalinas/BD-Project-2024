#!/usr/bin/python3
# Copyright (c) BDist Development Team
# Distributed under the terms of the Modified BSD License.
import os
from logging.config import dictConfig

from flask import Flask, jsonify, request
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool

from datetime import datetime, timedelta, time

# Use the DATABASE_URL environment variable if it exists, otherwise use the default.
# Use the format postgres://username:password@hostname/database_name to connect to the database.
DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://postgres:postgres@postgres/saude")

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={
        "autocommit": False,  # If True don’t start transactions automatically.
        "row_factory": namedtuple_row,
    },
    min_size=4,
    max_size=10,
    open=True,
    # check=ConnectionPool.check_connection,
    name="postgres_pool",
    timeout=5,
)

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(__name__)
app.config.from_prefixed_env()
log = app.logger

@app.route("/", methods=("GET",))
def clinic_index():
    """Show all the clinics."""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            clinics = cur.execute(
                """
                SELECT nome, morada
                FROM clinica;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    return jsonify(clinics)

@app.route("/c/<clinica>/", methods=("GET",))
def clinic_view(clinica):
    """Show all specialities offered by the medics in a specific clinic."""

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.especialidade
                FROM clinica c
                JOIN trabalha t ON c.nome = t.nome
                JOIN medico m ON t.nif = m.nif
                WHERE c.nome = %(clinica)s;
                """,
                {"clinica": clinica},
            )
            rows = cur.fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

            if rows:
                specialties = list({row[0].strip() for row in rows})  # Using a set to avoid duplicates and strip spaces
            else:
                specialties = []

    return jsonify(specialties)

def generate_time_slots(start, end, date):
    """Generate available time slots between start and end times for a specific date."""
    slots = []
    current = datetime.combine(date, start)
    end_time = datetime.combine(date, end)
    while current < end_time:
        slots.append(current)
        current += timedelta(minutes=30)
    return slots

def get_next_free_slots(now, booked_slots):
    """Get the next 3 free slots after 'now' not in 'booked_slots'."""
    working_hours_morning = generate_time_slots(time(8, 0), time(13, 0), now.date())
    working_hours_afternoon = generate_time_slots(time(14, 0), time(19, 0), now.date())
    working_hours = working_hours_morning + working_hours_afternoon

    free_slots = [slot for slot in working_hours if slot > now and slot not in booked_slots]
    
    return free_slots[:3]

@app.route("/c/<clinica>/<especialidade>/", methods=("GET",))
def doctor_clinic_specialty_view(clinica, especialidade):
    """Show all medics of a specific clinic for a specific specialty and their next 3 free schedules."""
    now = datetime.now()
    
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT m.nif, m.nome
                FROM clinica c
                JOIN trabalha t ON c.nome = t.nome
                JOIN medico m ON t.nif = m.nif
                WHERE c.nome = %(clinica)s AND m.especialidade = %(especialidade)s;
                """,
                {"clinica": clinica, "especialidade": especialidade},
            )
            rows = cur.fetchall()
            log.debug(f"Found {cur.rowcount} rows.")
            
            medics = []
            processed_nifs = set()
            for nif, nome in rows:
                if nif not in processed_nifs:
                    processed_nifs.add(nif)
                    cur.execute(
                        """
                        SELECT data, hora
                        FROM consulta
                        WHERE nif = %s AND data = %s
                        """,
                        (nif, now.date())
                    )
                    booked_slots = [datetime.combine(row[0], row[1]) for row in cur.fetchall()]
                    next_free_slots = get_next_free_slots(now, booked_slots)
                    # Convert datetime objects to string
                    next_free_slots_str = [slot.strftime('%Y-%m-%d %H:%M') for slot in next_free_slots]
                    medics.append({"name": nome.strip(), "next_free_slots": next_free_slots_str})
    
    return jsonify(medics)

@app.route("/a/<clinica>/registar/", methods=["POST",])
def create_appointment(clinica):
    """Create an appointment in the consulta table."""
    
    # Log the received parameters
    log.debug(f"Received request: paciente={request.args.get('paciente')}, medico={request.args.get('medico')}, data={request.args.get('data')}, hora={request.args.get('hora')}")

    paciente = request.args.get('paciente')
    medico = request.args.get('medico')
    data = request.args.get('data')
    hora = request.args.get('hora')

    if not all([paciente, medico, data, hora]):
        return jsonify({"error": "Missing required parameters"}), 400

    # Combine data and hora into a single datetime object
    try:
        appointment_datetime = datetime.strptime(f"{data} {hora}", "%Y-%m-%d %H:%M")
    except ValueError:
        return jsonify({"error": "Invalid date or time format"}), 400

    # Check if the appointment time is in the past
    if appointment_datetime < datetime.now():
        return jsonify({"error": "Cannot create an appointment in the past"}), 400

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                # Construct the SQL query string and log it
                sql_query = """
                    INSERT INTO consulta (ssn, nif, nome, data, hora)
                    VALUES (%s, %s, %s, %s, %s);
                    """
                log.debug(f"SQL query: {sql_query}")

                cur.execute(sql_query, (paciente, medico, clinica, data, hora))
                conn.commit()
                log.debug(f"Appointment created for paciente {paciente} with medico {medico} at clinic {clinica} on {data} at {hora}.")
            except Exception as e:
                conn.rollback()
                log.error(f"Error creating appointment: {str(e)}")
                return jsonify({"error": str(e)}), 500

    return jsonify({"message": "Appointment created successfully"}), 201

@app.route("/a/<clinica>/cancelar/", methods=["DELETE"])
def cancel_appointment(clinica):
    """Cancel an appointment in the consulta table."""
    
    paciente = request.args.get('paciente')
    medico = request.args.get('medico')
    data = request.args.get('data')
    hora = request.args.get('hora')

    if not all([paciente, medico, data, hora]):
        return jsonify({"error": "Missing required parameters"}), 400

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                # Check if the appointment exists
                cur.execute(
                    """
                    SELECT COUNT(*) FROM consulta
                    WHERE ssn = %s AND nif = %s AND nome = %s AND data = %s AND hora = %s;
                    """,
                    (paciente, medico, clinica, data, hora)
                )
                result = cur.fetchone()
                if result[0] == 0:
                    return jsonify({"error": "Appointment does not exist"}), 404

                # If exists, proceed to delete
                cur.execute(
                    """
                    DELETE FROM consulta
                    WHERE ssn = %s AND nif = %s AND nome = %s AND data = %s AND hora = %s;
                    """,
                    (paciente, medico, clinica, data, hora)
                )
                conn.commit()
                log.debug(f"Appointment cancelled for paciente {paciente} with medico {medico} at clinic {clinica} on {data} at {hora}.")
            except Exception as e:
                conn.rollback()
                log.error(f"Error cancelling appointment: {str(e)}")
                return jsonify({"error": str(e)}), 500

    return jsonify({"message": "Appointment cancelled successfully"}), 200


if __name__ == "__main__":
    app.run()
