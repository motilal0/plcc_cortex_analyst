from typing import Any, Dict, List, Optional
import pandas as pd
import requests
import snowflake.connector
import streamlit as st
import io

DATABASE = "CORTEX_ANALYST_DEMO_1"
SCHEMA = "REVENUE_TIMESERIES"
STAGE = "RAW_DATA"
FILE = "plcc_timeseries.yaml"
WAREHOUSE = "cortex_analyst_wh"

# Reading the entire content of the file
with open("snowflake_password.txt", 'r') as file:
    ps = file.read()

# replace values below with your Snowflake connection information
# HOST = "<host>"
ACCOUNT = "tz80493.us-east-2.aws"
USER = "motilal"
PASSWORD = ps
ROLE = "SYSADMIN"

if 'CONN' not in st.session_state or st.session_state.CONN is None:
    st.session_state.CONN = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT,
        # host=HOST,
        port=443,
        warehouse=WAREHOUSE,
        role=ROLE,
    )

def send_message(prompt: str) -> Dict[str, Any]:
    """Calls the REST API and returns the response."""
    request_body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
    }
    resp = requests.post(
        url=f"https://{ACCOUNT}.snowflakecomputing.com/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
    )
    request_id = resp.headers.get("X-Snowflake-Request-Id")
    if resp.status_code < 400:
        return {**resp.json(), "request_id": request_id}  # type: ignore[arg-type]
    else:
        raise Exception(
            f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}"
        )

def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(prompt=prompt)
            request_id = response["request_id"]
            content = response["message"]["content"]
            display_content(content=content, request_id=request_id)  # type: ignore[arg-type]
    st.session_state.messages.append(
        {"role": "assistant", "content": content, "request_id": request_id}
    )

def df_to_excel(df: pd.DataFrame) -> bytes:
    """Converts DataFrame to Excel bytes, ensuring all columns (excluding the index) are included."""
    # Reset the index to make sure the first column is treated as a regular column
    df = df.reset_index(drop=True)  # `drop=True` ensures the old index is not added as a column
    
    # Create an Excel file in memory
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")  # `index=False` prevents writing the index
    output.seek(0)  # Rewind the file-like object
    return output.getvalue()  # Return the bytes

def display_content(
    content: List[Dict[str, str]],
    request_id: Optional[str] = None,
    message_index: Optional[int] = None,
) -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    if request_id:
        with st.expander("Request ID", expanded=False):
            st.markdown(request_id)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    df = pd.read_sql(item["statement"], st.session_state.CONN)
                    # Display the DataFrame
                    st.dataframe(df)
                    
                    # Add Download Buttons for CSV and Excel with unique keys
                    st.download_button(
                        label="Download Data as CSV",
                        data=df_to_csv(df),
                        file_name="query_results.csv",
                        mime="text/csv",
                        key=f"download_csv_{message_index}"  # Unique key for CSV button
                    )
                    st.download_button(
                        label="Download Data as Excel",
                        data=df_to_excel(df),
                        file_name="query_results.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"download_excel_{message_index}"  # Unique key for Excel button
                    )

def df_to_csv(df: pd.DataFrame) -> bytes:
    """Converts DataFrame to CSV bytes, ensuring all columns (excluding the index) are included."""
    # Reset the index to make sure the first column is treated as a regular column
    df = df.reset_index(drop=True)  # `drop=True` ensures the old index is not added as a column
    
    # Return CSV with all columns, excluding the index
    return df.to_csv(index=False, header=True).encode()  # Set index=False to not write the old index again


st.title("PLCC Conversational BI")
st.markdown(f"Semantic Model: `{FILE}`")

if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None

for message_index, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        display_content(
            content=message["content"],
            request_id=message.get("request_id"),
            message_index=message_index,
        )

if user_input := st.chat_input("What is your question?"):
    process_message(prompt=user_input)

if st.session_state.active_suggestion:
    process_message(prompt=st.session_state.active_suggestion)
    st.session_state.active_suggestion = None