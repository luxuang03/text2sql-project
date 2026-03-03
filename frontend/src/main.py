import os
from urllib.parse import quote

import requests
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# URL backend DENTRO Docker
BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000").rstrip("/")

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


@app.get("/schema", response_class=HTMLResponse)
def schema(request: Request):
    try:
        r = requests.get(f"{BACKEND_URL}/schema_summary", timeout=10)
        r.raise_for_status()
        schema = r.json()
        return templates.TemplateResponse(
            "schema.html",
            {"request": request, "schema": schema}
        )
    except Exception as e:
        return templates.TemplateResponse(
            "schema.html",
            {"request": request, "schema": [], "error": str(e)}
        )


@app.post("/search", response_class=HTMLResponse)
def search(request: Request, question: str = Form(...)):
    try:
        q = quote(question)
        r = requests.get(f"{BACKEND_URL}/search/{q}", timeout=10)
        r.raise_for_status()
        results = r.json()
        return templates.TemplateResponse(
            "search.html",
            {
                "request": request,
                "question": question,
                "results": results
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "search.html",
            {
                "request": request,
                "question": question,
                "results": [],
                "error": str(e)
            }
        )


@app.post("/add", response_class=HTMLResponse)
def add(request: Request, data_line: str = Form(...)):
    try:
        r = requests.post(
            f"{BACKEND_URL}/add",
            json={"data_line": data_line},
            timeout=10
        )

        if r.status_code == 200:
            return templates.TemplateResponse(
                "add.html",
                {
                    "request": request,
                    "ok": True,
                    "status": r.status_code,
                    "body": r.json()
                }
            )
        else:
            return templates.TemplateResponse(
                "add.html",
                {
                    "request": request,
                    "ok": False,
                    "status": r.status_code,
                    "body": r.text
                }
            )
    except Exception as e:
        return templates.TemplateResponse(
            "add.html",
            {
                "request": request,
                "ok": False,
                "status": None,
                "body": str(e)
            }
        )
