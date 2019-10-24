FROM conda/miniconda3:latest

# install ubuntu deps
RUN apt-get -q update \
	&& apt-get -q -y install --no-install-recommends git-core wget build-essential > /dev/null \
	&& apt-get -q clean \
    && rm -rf /var/lib/apt/lists/*

# install gams
RUN wget -q https://d37drm4t2jghv5.cloudfront.net/distributions/24.9.2/linux/linux_x64_64_sfx.exe \
    && chmod 755 linux_x64_64_sfx.exe \
    && ./linux_x64_64_sfx.exe > /dev/null \
	&& rm ./linux_x64_64_sfx.exe
ENV PATH $PATH:$PWD/gams24.9_linux_x64_64_sfx

# install ixmp deps
RUN conda config --add channels conda-forge \
	&& conda update -q --yes conda \
	&& conda env create -f /d2ix/environment.yml -q \
    && conda clean --all --yes

# install d2ix
COPY . /d2ix
RUN /bin/bash -c 'source activate d2ix && cd /d2ix && pip install .'
WORKDIR /