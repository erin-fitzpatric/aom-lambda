FROM public.ecr.aws/lambda/python:3.11

COPY civs_stats.py requirements.txt ./

RUN pip install -r requirements.txt -t ${LAMBDA_TASK_ROOT}

COPY civs_stats.py ${LAMBDA_TASK_ROOT}
COPY __init__.py ${LAMBDA_TASK_ROOT}

CMD [ "civs_stats.lambda_handler" ]