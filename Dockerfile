# Usamos Python 3.13 porque Spark 4.1.1 soporta Python 3.10+.
# La imagen slim es más ligera que una imagen completa.
FROM python:3.13-slim-bookworm

# Evita que Python genere archivos .pyc y ayuda a mostrar logs inmediatamente.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instalamos Java 17 porque Spark necesita Java.
# También instalamos curl y procps para diagnósticos básicos dentro del contenedor.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    procps \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Definimos JAVA_HOME para que Spark encuentre Java correctamente.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Carpeta de trabajo dentro del contenedor.
WORKDIR /app

# Copiamos primero requirements.txt para aprovechar la caché de Docker.
COPY requirements.txt /app/requirements.txt

# Instalamos las dependencias Python.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Por defecto, el contenedor quedará disponible para ejecutar comandos.
CMD ["bash"]