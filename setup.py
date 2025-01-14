from setuptools import setup, find_packages

setup(
    name="fairb",  # Nombre del paquete
    version="0.0.0",  # Versión del paquete
    author="Diego Ramírez González",  # Nombre del autor
    author_email="diegogz95@gmail.com",  # Email del autor
    description="Una breve descripción de tu paquete",
    long_description=open("README.md").read(),  # Descripción larga, como aparece en PyPI
    long_description_content_type="text/markdown",  # Formato del README
    # url="https://github.com/tu_usuario/mi_paquete",  # URL del repositorio
    # license="MIT",  # Licencia
    packages=find_packages(),  # Detecta automáticamente los subpaquetes
    install_requires=[
        "numpy>=1.24.3",  # Ejemplo de dependencias
        "pandas>=2.0.1",
        "datalad>=1.1.3",
        "datalad-container>=1.2.5",
        "filelock>=3.12"
    ],
    python_requires=">=3.11",  # Versión mínima de Python
    # classifiers=[  # Clasificación para PyPI
    #     "Programming Language :: Python :: 3",
    #     "License :: OSI Approved :: MIT License",
    #     "Operating System :: OS Independent",
    # ],
    entry_points={
        "console_scripts": [
            "fairb=fairb.__main__:main",
        ],
    },
)
