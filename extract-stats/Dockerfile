FROM public.ecr.aws/lambda/python:3.11

COPY civs_stats.py ${LAMBDA_TASK_ROOT}

CMD [ "civs_stats.lambda_handler" ]