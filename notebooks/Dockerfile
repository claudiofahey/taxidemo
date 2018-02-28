FROM continuumio/anaconda3

RUN conda install -y jupyter

RUN \
    conda install -y \
        pymysql \
        pyramid \
        seaborn && \
    pip install \
        yapsy \
        simplejson \
        colander \
        python-pptx \
        elasticsearch \
        elasticsearch-dsl &&\
    git clone https://github.com/pifantastic/python-rison && \
    cd python-rison/ && \
    python ./setup.py install

RUN \
    conda install -c conda-forge jupyter_contrib_nbextensions && \
    jupyter nbextensions_configurator enable --sys-prefix

# See https://github.com/dunovank/jupyter-themes
RUN \
    pip install jupyterthemes && \
    jt -t onedork -fs 95 -altp -tfs 11 -nfs 115 -cellw 88% -T

RUN \
    conda install -c conda-forge \
        pyspark

# Below from https://github.com/gettyimages/docker-spark/blob/master/Dockerfile.
ARG JAVA_MAJOR_VERSION=8
ARG JAVA_UPDATE_VERSION=131
ARG JAVA_BUILD_NUMBER=11
ENV JAVA_HOME /usr/jdk1.${JAVA_MAJOR_VERSION}.0_${JAVA_UPDATE_VERSION}
ENV PATH $PATH:$JAVA_HOME/bin
RUN curl -sL --retry 3 --insecure \
  --header "Cookie: oraclelicense=accept-securebackup-cookie;" \
  "http://download.oracle.com/otn-pub/java/jdk/${JAVA_MAJOR_VERSION}u${JAVA_UPDATE_VERSION}-b${JAVA_BUILD_NUMBER}/d54c1d3a095b4ff2b6607d096fa80163/server-jre-${JAVA_MAJOR_VERSION}u${JAVA_UPDATE_VERSION}-linux-x64.tar.gz" \
  | gunzip \
  | tar x -C /usr/ \
  && ln -s $JAVA_HOME /usr/java \
  && rm -rf $JAVA_HOME/man

COPY *.ipynb /notebooks/original/

EXPOSE 8888

CMD jupyter notebook --notebook-dir=/notebooks --ip='*' --port=8888 --no-browser --allow-root --NotebookApp.token=
