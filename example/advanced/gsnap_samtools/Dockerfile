FROM ubuntu

# Fetch and install latest updates
RUN apt-get update -qq

# Install all necessary tools (curl, gcc, make). And yes, we also require Perl!
RUN apt-get install -y -qq curl gcc make perl zlib1g zlib1g-dev

ARG GSNAP_VERSION=2017-05-08
ARG GSNAP_TAR_BALL=gmap-gsnap-${GSNAP_VERSION}
ARG GSNAP_DIRECTORY=gmap-${GSNAP_VERSION}

# Download gsnap and set it up in /opt/gmap-<date>/
RUN curl -sL http://research-pub.gene.com/gmap/src/${GSNAP_TAR_BALL}.tar.gz \
  | gunzip \
  | tar -x -C /opt

RUN ls -lah /opt/
RUN ls -lah /opt/${GSNAP_DIRECTORY}/

RUN cd /opt/${GSNAP_DIRECTORY}/ \
  && sh configure --prefix=/opt/${GSNAP_DIRECTORY}/ --enable-zlib \
  && make \
  && make install

ENV PATH /opt/${GSNAP_DIRECTORY}/bin:$PATH

# samtools
RUN apt-get install -y -qq curl gcc make zlib1g zlib1g-dev libbz2-dev bzip2 liblzma-dev

RUN curl -sL https://github.com/samtools/samtools/releases/download/1.5/samtools-1.5.tar.bz2 \
        | tar -xj -C /opt/

RUN cd /opt/samtools*/ \
        && sh configure --without-curses \
        && make \
        && make install
