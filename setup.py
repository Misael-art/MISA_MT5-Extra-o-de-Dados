from setuptools import setup, find_packages

setup(
    name="mt5_extracao",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "MetaTrader5>=5.0.29",
        "pandas>=1.3.0,<2.1.0",
        "pandas_ta>=0.3.14b",
        "numpy>=1.20.0",
        "matplotlib>=3.3.0",
        "sqlalchemy>=1.4.0",
        "Pillow>=8.0.0",
        "psutil>=5.8.0",
    ],
    author="Desenvolvedor",
    author_email="dev@example.com",
    description="Aplicação para extração de dados do MetaTrader 5",
    keywords="metatrader5, trading, finanças, análise técnica",
    python_requires=">=3.7",
) 