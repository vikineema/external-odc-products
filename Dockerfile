FROM digitalearthafrica/deafrica-sandbox:sudo-0.0.9

USER root
COPY jupyter_lab_config.py /etc/jupyter/

USER $NB_USER
RUN mkdir -p $HOME/workspace
WORKDIR $HOME/workspace
COPY . $HOME/workspace
RUN pip install $HOME/workspace