# Contributing to Big Data Analytics Project

Thank you for your interest in contributing to this project! This guide will help you get started.

## Ways to Contribute

1. **Report bugs or issues**
2. **Suggest new features or improvements**
3. **Add new example scripts**
4. **Improve documentation**
5. **Add support for new technologies**
6. **Optimize Docker configurations**

## Development Setup

### 1. Fork and Clone
```bash
git clone https://github.com/YOUR_USERNAME/big-data-analytics.git
cd big-data-analytics
```

### 2. Create a Branch
```bash
git checkout -b feature/your-feature-name
```

### 3. Make Changes
- Follow the existing code style
- Add comments for complex logic
- Update documentation as needed

### 4. Test Your Changes
```bash
# Build and test locally
docker compose build
docker compose up -d

# Run example scripts
make test-all
```

### 5. Commit and Push
```bash
git add .
git commit -m "Description of your changes"
git push origin feature/your-feature-name
```

### 6. Create Pull Request
- Go to the original repository
- Click "New Pull Request"
- Select your branch
- Describe your changes

## Code Style Guidelines

### Python
- Follow PEP 8 style guide
- Use meaningful variable names
- Add docstrings to functions and classes
- Include type hints where appropriate

### Docker
- Use official base images when possible
- Minimize layer count
- Clean up in the same layer as installation
- Document exposed ports and volumes

### Documentation
- Use clear, concise language
- Include code examples
- Keep README files up to date
- Add comments for complex configurations

## Adding New Technologies

To add a new big data technology:

1. **Create a new directory**
   ```bash
   mkdir new-tech
   ```

2. **Add Dockerfile**
   ```dockerfile
   FROM base-image
   # Installation steps
   # Configuration
   # Expose ports
   ```

3. **Add configuration files**
   ```bash
   mkdir new-tech/config
   # Add configuration files
   ```

4. **Update docker-compose.yml**
   ```yaml
   new-tech:
     build:
       context: ./new-tech
     ports:
       - "PORT:PORT"
     networks:
       - bigdata-network
   ```

5. **Create example script**
   ```python
   # scripts/new_tech_example.py
   # Add working example
   ```

6. **Add README**
   ```markdown
   # new-tech/README.md
   # Document usage and examples
   ```

7. **Update main README**
   - Add to technologies list
   - Document ports and usage
   - Add to quickstart guide

## Testing Checklist

Before submitting a pull request:

- [ ] Code builds without errors
- [ ] All containers start successfully
- [ ] Example scripts run without errors
- [ ] Documentation is updated
- [ ] No sensitive information in commits
- [ ] Follows project code style
- [ ] Tests pass (if applicable)

## Adding Example Scripts

Example scripts should:
1. Be well-documented with docstrings
2. Include error handling
3. Demonstrate key features of the technology
4. Be runnable with minimal setup
5. Print informative output

Example template:
```python
#!/usr/bin/env python3
"""
Technology Name Example
Brief description of what this script demonstrates
"""

def main():
    """
    Main function with clear description
    """
    # Your code here
    print("Starting example...")
    # ...
    print("Example completed!")

if __name__ == "__main__":
    main()
```

## Documentation Guidelines

- Use markdown for all documentation
- Include code blocks with syntax highlighting
- Add screenshots for UI features
- Keep line length under 100 characters
- Use tables for structured information
- Link to official documentation

## Commit Message Guidelines

Use clear, descriptive commit messages:

```
Add Kafka consumer example script

- Implements consumer with error handling
- Adds documentation
- Updates README with usage instructions
```

Format:
- First line: Brief summary (50 chars or less)
- Blank line
- Detailed description with bullet points

## Pull Request Guidelines

Your PR should:
- Have a clear title and description
- Reference related issues
- Include screenshots if UI changes
- Pass all checks
- Be reviewed by at least one maintainer

## Questions?

- Open an issue for discussion
- Check existing issues and PRs
- Review the main README and documentation

## Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Help others learn
- Focus on what is best for the project

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT License).

Thank you for contributing! 🎉
