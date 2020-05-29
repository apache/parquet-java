FROM lyft/java8:19f8cb1ba62f3efef7c4939e5a54699a1be988de
ARG IMAGE_VERSION
ENV IMAGE_VERSION=${IMAGE_VERSION}
COPY manifest.yaml /code/lyft-hive/manifest.yaml
COPY . /code/lyft-hive
WORKDIR /code/lyft-hive

RUN mvn -DskipTests -Dcheckstyle.skip=true clean test-compile install
