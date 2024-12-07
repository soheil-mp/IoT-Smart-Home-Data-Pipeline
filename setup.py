from setuptools import setup, find_packages

setup(
    name="iot_pipeline",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "psycopg2-binary>=2.9.9",
        "python-dotenv>=1.0.0",
        "pandas>=2.1.3",
        "numpy>=1.26.2",
        "SQLAlchemy>=2.0.25",
        "streamlit>=1.28.2",
        "plotly>=5.18.0",
    ],
    python_requires=">=3.10",
) 