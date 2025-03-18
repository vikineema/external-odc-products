FROM digitalearthafrica/deafrica-sandbox:0.0.9

ENV GS_NO_SIGN_REQUEST=YES

ARG NB_USER="jovyan"
ARG NB_UID="1000"
ARG NB_GID="100"

USER root
COPY jupyter_lab_config.py /etc/jupyter/
RUN mkdir -p $HOME/workspace
COPY . $HOME/workspace
RUN chown -R $NB_UID:$NB_GID $HOME/workspace

USER $NB_USER
RUN pip install -e $HOME/workspace
WORKDIR $HOME/workspace
