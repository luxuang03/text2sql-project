from pydantic import BaseModel
from typing import Any, List, Optional, Literal

class PropertyItem(BaseModel):
    property_name: str
    property_value: Any

class ResultItem(BaseModel):
    item_type: str
    properties: List[PropertyItem]

class SqlSearchRequest(BaseModel):
    sql_query: str

class SearchRequest(BaseModel):
    question: str

class SqlSearchResponse(BaseModel):
    sql: str
    sql_validation: Literal["valid", "unsafe", "invalid"]
    results: Optional[List[ResultItem]]

class AddRequest(BaseModel):
    data_line: str

class AddResponse(BaseModel):
    status: str = "ok"

class SchemaRow(BaseModel):
    table_name: str
    table_column: str