ARG base_image
FROM ${base_image}

# install d2ix
COPY . /d2ix
RUN /bin/bash -c 'source activate d2ix && cd /d2ix && python setup.py install'
RUN /bin/bash -c 'source activate d2ix && conda install -c conda-forge mypy --yes'
WORKDIR /