#Create Conda Environment
conda env create -f environment.yml

#Activate Conda Env
conda activate snowflake-streamlit

#Check Python Version
python -V

#Run Streamlit APP
streamlit run Dashboard.py