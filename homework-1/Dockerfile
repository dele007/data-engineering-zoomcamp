FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml ./

RUN pip install --upgrade pip
RUN pip install pandas sqlalchemy psycopg2-binary


COPY hw-1-notebook.py .

ENTRYPOINT ["python","hw-1-notebook.py"]