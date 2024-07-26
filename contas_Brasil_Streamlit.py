import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from snowpark_session import create_session_object
from snowflake.snowpark.functions import col

# Write directly to the app
st.title(":bank: Controle de Contas do Brasil! :bank:")
st.write(
    """Escolha a categoria de conta a lancar: 
    """
)

session = create_session_object()

my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
my_dataframe = my_dataframe.distinct()
st.dataframe(data=my_dataframe, use_container_width=True)

tipo_conta = st.selectbox(
    "Nome do Tipo de Conta: ",
    my_dataframe,
    index=None,
    placeholder='Escolha uma Opcao:'
    )

if tipo_conta: 
    st.write("O tipo de Conta a lancar será: ", tipo_conta)


    #ingredients_string = ''

    #for fruit_chosen in ingredients_list:
    #    ingredients_string += fruit_chosen + ' '

    #st.write(ingredients_string)

    #my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order)
    #        values ('""" + ingredients_string + """','"""+name_on_order+ """')"""

    #st.write(my_insert_stmt)
    #time_to_insert = st.button('Submit Order')

    #if time_to_insert:
    #    session.sql(my_insert_stmt).collect()

    #    st.success('Your Smoothie is ordered, '+name_on_order+'!', icon="✅")
    

