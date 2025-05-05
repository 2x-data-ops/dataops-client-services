import streamlit as st
from scrape import DataPipeline
import pandas as pd
import logging
from collections import deque

class StreamlitHandler(logging.Handler):
    def __init__(self, placeholder, max_lines=1000):
        super().__init__()
        self.placeholder = placeholder
        self.max_lines = max_lines
        self.log_lines = deque(maxlen=max_lines)  # Use deque to automatically limit lines

    def emit(self, record):
        # Format and add the new log entry
        log_entry = self.format(record)
        self.log_lines.append(log_entry)
        
        # Join the most recent logs and update display
        log_text = '\n'.join(self.log_lines)
        # Use empty() to avoid flickering and clear previous content
        self.placeholder.empty()
        self.placeholder.code(log_text, language='plaintext')

def main():
    st.title("Data Catalog Pipeline Automation")
    
    # Create pipeline instance
    pipeline = DataPipeline()
    
    # Create a placeholder for logs
    log_container = st.container()
    log_placeholder = log_container.empty()
    
    # Add custom streamlit handler to the logger
    streamlit_handler = StreamlitHandler(log_placeholder)
    streamlit_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    pipeline.logger.addHandler(streamlit_handler)
    
    # Create run button
    if st.button("Run Pipeline"):
        try:
            with st.spinner('Running pipeline...'):
                pipeline.run()
                st.success("Pipeline completed successfully!")
                
                # Show data preview
                try:
                    df = pd.read_csv('output/4_final_output.csv')
                    st.subheader("Preview of processed data:")
                    st.dataframe(df.head(100))
                except Exception as e:
                    st.warning(f"Pipeline completed but couldn't load preview: {str(e)}")
                
        except Exception as e:
            st.error(f"Pipeline failed: {str(e)}")

if __name__ == "__main__":
    main()