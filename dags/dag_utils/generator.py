from functools import wraps

from airflow.models import Variable


def generator(wrapped):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            func_name = fn.__qualname__ if fn.__qualname__ else fn.__name__
            dag_configurations = Variable.get(func_name, default_var={}, deserialize_json=True)
            if not dag_configurations:
                print(f'[WARNING] Missing config file for DAG {func_name}. '
                      f'There are no instance of DAG {func_name} is scheduled')
            else:
                # TODO: validate configs file
                print(f'There are {len(dag_configurations)} instances of DAG {func_name} is scheduled')

            for instance_id, params in dag_configurations.items():
                kwargs.update(params)
                fn(dag_id=f'{func_name}_{instance_id}', *args, **kwargs)

        return wrapper

    return decorator(wrapped)
