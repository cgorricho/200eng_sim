import streamlit as st
import pandas as pd
from datetime import datetime
from streamlit_autorefresh import st_autorefresh


# Main function for the Streamlit app
def main():
    # Check if the DataFrame exists in session state; if not, initialize it
    if 'data' not in st.session_state:
        st.session_state.data = pd.DataFrame(columns=['Timestamp', 'Value'])

    # Append a new row with the current timestamp and a sample value
    new_row = {'Timestamp': datetime.now(), 'Value': len(st.session_state.data) + 1}
    st.session_state.data = pd.concat([st.session_state.data, pd.DataFrame(new_row, index=[0])], ignore_index=True)

    # Display the DataFrame
    st.write(st.session_state.data)

    # Set up auto-refresh every 5 seconds
    st_autorefresh(interval=5000, key="data_refresh")

# Run the app
if __name__ == "__main__":
    main()
