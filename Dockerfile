FROM bellsoft/liberica-openjdk-alpine-musl:11 AS coordinator-build
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
RUN javac utils/*.java server/*.java client/*.java

FROM bellsoft/liberica-openjdk-alpine-musl:11 AS client-build
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
RUN javac utils/*.java client/*.java server/*.java

FROM bellsoft/liberica-openjdk-alpine-musl:11 AS server-build
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
RUN javac utils/*.java client/*.java server/*.java