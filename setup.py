from setuptools import setup

setup(name='bunnylink',
      version='0.1',
      description='Python to RabbitMQ utilities using Pika',
      maintainer='Liviu Manea',
      maintainer_email='mlmarius@yahoo.com',
      packages=['bunnylink'],
      license='BSD',
      install_requires=[
          'pika>=0.10.0',
      ],
      # dependency_links=[
      #     # 'git+ssh://git@github.com/mlmarius/pika.git@64f8f12#egg=pika-0.10.0',
      #     'git+ssh://git@github.com/mlmarius/pika.git@64f8f12#egg=pika'
      # ],
      package_data={'': ['LICENSE', 'README.md']},
      classifiers=[],
      zip_safe=True)
