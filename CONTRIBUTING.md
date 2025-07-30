Contributing to Big-Data-Learners-Hub

First off, thank you for considering contributing! This project is made possible by a community of volunteers like you. Your contributions will help create a valuable resource for data engineering learners everywhere.

This document provides guidelines for contributing to the project. Please feel free to propose changes to this document in a pull request.
Code of Conduct

This project and everyone participating in it is governed by a Code of Conduct. By participating, you are expected to uphold this code. Please report unacceptable behavior.
How Can I Contribute?

There are many ways to contribute to the project, and every contribution is appreciated.
🐛 Reporting Bugs

If you find a bug in the source code, you can help us by submitting an issue to our GitHub Repository. Even better, you can submit a Pull Request with a fix.
✨ Suggesting Enhancements

If you have an idea for an enhancement, you can submit an issue with your suggestion. Please provide as much detail and context as possible.
Pull Request Process

    Fork the repo and create your branch from main.

    If you've added code that should be tested, add tests.

    Ensure your code lints and is well-commented.

    Issue that pull request!

Types of Contributions We're Looking For

Here are some specific ways you can contribute to the projects in this repository.
1. Query and Code Optimization

    Optimize Queries: Take an existing solution (e.g., a HiveQL or Spark SQL query) and optimize it for performance or cost. Add comments explaining the "before" and "after" and why your changes make it better.

    Improve the Cleaning Process: Enhance the data cleaning and preprocessing scripts. This could involve handling edge cases more robustly, improving performance, or making the code more readable and modular.

    Add Implementations in New Technologies: If a project has a solution in PySpark, add one in Scala/Spark, or vice-versa. Or maybe try a solution using a different tool entirely!

2. Data Storage and Architecture Enhancements

    Optimize Data Storage: Implement solutions that use efficient, columnar file formats like Parquet, Avro, or ORC. Add a README in the project folder explaining the benefits of the chosen format for that specific use case (e.g., schema evolution, compression, predicate pushdown).

    Implement Partitioning and Bucketing: This is a key optimization strategy! Add examples of how to use partitioning and bucketing to speed up queries. For example, in the credit card transaction project, you could partition the data by transaction_date or card_type. Explain your strategy in the project's README.

    Containerize a Project: Create a Dockerfile and docker-compose.yml for a project to make it easy for others to run the code and its dependencies (like a Spark cluster) in a containerized environment.

3. New Projects and Features

    Add New Project Ideas: Have a cool dataset and a problem statement for a new mini-project? Propose it by creating an issue!

    Add Data Visualizations: Create notebooks (e.g., Jupyter) or scripts that visualize the results of the analysis.

    Improve Documentation: See a typo? Think a README could be clearer? Fix it! Good documentation is as important as good code.

We look forward to your contributions
