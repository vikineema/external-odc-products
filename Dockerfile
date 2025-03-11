FROM digitalearthafrica/deafrica-sandbox:sudo-0.0.9

USER root
ENV GS_NO_SIGN_REQUEST=YES
COPY jupyter_lab_config.py /etc/jupyter/

USER $NB_USER
RUN mkdir -p $HOME/workspace
COPY . $HOME/workspace
RUN pip install -e $HOME/workspace
WORKDIR $HOME/workspace