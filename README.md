# mnesia_builder

mnesia_builder is an experimental project designed for fun and exploration of the possibilities with Mnesia, the distributed database in Erlang/OTP. It enables users to model, test, and dynamically interact with Mnesia table schemas. Programs can create tables dynamically without a pre-conceived understanding of the table schemas. It offers a unique way to learn and experiment with Mnesia's capabilities.

Features

- Model multiple table schemas.
- Define attributes and fields for tables.
- Set field attributes including types.
- Validate table schemas.
- Auto-generate code for table operations.
- Bind generated modules into “behaviour” modules for common database operations.
- Dynamically build, deploy, use, and destroy Mnesia tables.

Why Use It?

- This project is purely experimental and created for the joy of building and exploring. It’s an opportunity to learn, test ideas, and see what’s possible with Mnesia in a dynamic way.
- There are few possible use cases other than academic experimentation:
     (1) Easily model and test various table schema configurations before deciding on final specifications for coding and deployment
     (2) Dynamic table definition, creation, usage, and optionally destruction allowing applications to manage their data when data models are unpredictable during runtime
     (3) Integrating Mnesia into your application if you do not know Erlang (requires serialization libraries for other programming languages, on the TODO list)

Getting Started

- Note: This project is currently under development, and installation instructions are not yet available.

Prerequisites

- Erlang/OTP installed on your machine.
- Basic familiarity with Mnesia and Erlang.

Usage

Once completed, mnesia_builder will provide:

- Tools to define table schemas.
- A code generator for Mnesia tables.
- Dynamic deployment capabilities for Mnesia table operations.

Stay tuned for detailed installation and usage instructions.

License

- This project is licensed under the MIT License - see the LICENSE file for details.

Disclaimer

- This project is provided "as is" without any warranties. Use it at your own risk. It is experimental and not suitable for production environments.

Contributing

- Contributions are welcome! If you have ideas or enhancements, feel free to open issues or submit pull requests.

Contact

- For questions or feedback, feel free to contact the creator at [Your Email Address].


