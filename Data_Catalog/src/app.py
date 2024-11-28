import streamlit as st
from scrape import DataPipeline
import pandas as pd
import logging

class StreamlitHandler(logging.Handler):
    def __init__(self, placeholder):
        super().__init__()
        self.placeholder = placeholder
        self.log_text = ""

    def emit(self, record):
        # Format the log message
        log_entry = self.format(record)
        self.log_text += log_entry + "\n"
        # Update the streamlit component with all logs
        self.placeholder.text_area("Logs:", self.log_text, height=400)

def main():
    st.title("Data Catalog Pipeline Automation")
    
    # Create pipeline instance
    pipeline = DataPipeline()
    
    # Create a placeholder for logs
    log_placeholder = st.empty()
    
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
                    st.dataframe(df.head())
                except Exception as e:
                    st.warning(f"Pipeline completed but couldn't load preview: {str(e)}")
                
        except Exception as e:
            st.error(f"Pipeline failed: {str(e)}")

if __name__ == "__main__":
    main()