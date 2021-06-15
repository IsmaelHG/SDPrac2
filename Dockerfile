FROM ibmfunctions/action-python-v3.7

RUN pip install \
--upgrade pip \
urllib3==1.24.2 \
setuptools \
lithops \
tweepy \
vaderSentiment \
pandas \
pandasql \
mtranslate \
sqlalchemy \
requests