# 🤝 Contributing to Carbon Foodprint Scanner

Thank you for your interest in contributing to Carbon Foodprint Scanner! This document provides guidelines for contributing to the project.

## 🎯 How to Contribute

### **Types of Contributions**

We welcome contributions in the following areas:

- **🐛 Bug Reports** - Help us identify and fix issues
- **✨ Feature Requests** - Suggest new features or improvements
- **📚 Documentation** - Improve documentation and examples
- **🧪 Testing** - Add tests or improve test coverage
- **🔧 Code Improvements** - Refactor code or improve performance
- **📊 Data Quality** - Improve data analysis and reporting

### **Getting Started**

1. **Fork the repository**
   ```bash
   git clone https://github.com/Ladmya/carbon-foodprint-scanner.git
   cd carbon-foodprint-scanner
   ```

2. **Set up your environment**
   ```bash
   python -m venv carbon_foodprint-venv
   source carbon_foodprint-venv/bin/activate  # On Windows: carbon_foodprint-venv\Scripts\activate
   pip install -r requirements.txt
   make setup
   ```

3. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

## 📋 Development Guidelines

### **Code Style**

- Follow **PEP 8** Python style guidelines
- Use **type hints** for function parameters and return values
- Write **docstrings** for all functions and classes
- Keep functions **small and focused**

### **Testing**

- Write tests for new features (test structure available)
- Ensure all tests pass before submitting
- Run the test suite:
  ```bash
  make test
  make test-unit      # Future development
  make test-integration # Future development
  ```

### **Data Engineering Contributions**

When contributing to data engineering components:

1. **Follow the ETL pipeline structure**:
   - Extract: `data_engineering/scripts/collection/`
   - Transform: `data_engineering/scripts/analysis/`
   - Load: `data_engineering/scripts/loading/`

2. **Update documentation**:
   - Update relevant README files
   - Document new scripts and their purpose
   - Update Makefile commands if needed

3. **Maintain data quality**:
   - Add data validation where appropriate
   - Include error handling for edge cases
   - Generate appropriate reports

### **Bot Contributions**

When contributing to the Telegram bot:

1. **Follow the bot structure**:
   - Main bot: `src/food_scanner/bot/run_telegram_bot.py`
   - Barcode scanning: `src/food_scanner/bot/barcode_scanner.py`
   - Database service: `src/food_scanner/bot/database_service.py`

2. **Test bot functionality**:
   ```bash
   make test-bot
   ```

3. **Handle errors gracefully**:
   - Provide user-friendly error messages
   - Log errors appropriately
   - Implement fallback mechanisms

## 🚀 Submitting Changes

### **Commit Guidelines**

Use conventional commit messages:

```bash
# Format: type(scope): description
git commit -m "feat(analysis): add new data quality metrics"
git commit -m "fix(bot): resolve barcode scanning timeout issue"
git commit -m "docs(readme): update installation instructions"
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Test additions or changes
- `chore`: Maintenance tasks

### **Pull Request Process**

1. **Update your branch**
   ```bash
   git fetch origin
   git rebase origin/main
   ```

2. **Test your changes**
   ```bash
   make setup
   make test          # Basic test structure
   make analyze-raw   # If data engineering changes
   ```

3. **Create a Pull Request**
   - Provide a clear description of your changes
   - Include any relevant issue numbers
   - Add screenshots for UI changes
   - Update documentation if needed

4. **PR Template**
   ```markdown
   ## Description
   Brief description of changes

   ## Type of Change
   - [ ] Bug fix
   - [ ] New feature
   - [ ] Documentation update
   - [ ] Code refactoring
   - [ ] Test addition

   ## Testing
   - [ ] All tests pass
   - [ ] New tests added for new features
   - [ ] Manual testing completed

   ## Checklist
   - [ ] Code follows style guidelines
   - [ ] Documentation updated
   - [ ] No breaking changes
   ```

## 🐛 Reporting Issues

### **Bug Reports**

When reporting bugs, please include:

1. **Environment details**:
   - Python version
   - Operating system
   - Dependencies versions

2. **Steps to reproduce**:
   - Clear step-by-step instructions
   - Sample data if applicable

3. **Expected vs actual behavior**:
   - What you expected to happen
   - What actually happened

4. **Error messages**:
   - Full error traceback
   - Log files if available

### **Feature Requests**

When requesting features, please include:

1. **Problem description**:
   - What problem does this solve?
   - Why is this feature needed?

2. **Proposed solution**:
   - How should this feature work?
   - Any design considerations?

3. **Use cases**:
   - Who would use this feature?
   - What are the main use cases?

## 📚 Documentation

### **Adding Documentation**

- Update relevant README files
- Add docstrings to new functions
- Include examples for new features
- Update architecture diagrams if needed

### **Documentation Structure**

```
docs/
├── api/              # API documentation
├── bot/              # Bot usage guide
├── data/             # Data pipeline documentation
└── deployment/       # Deployment guides
```

## 🏗️ Architecture Guidelines

### **Data Engineering**

- Keep ETL pipeline modular and testable
- Use consistent naming conventions
- Implement proper error handling
- Generate comprehensive reports

### **Bot Development**

- Follow Telegram Bot API best practices
- Implement proper state management
- Handle user input validation
- Provide clear user feedback

### **Code Organization**

- Keep related functionality together
- Use meaningful file and folder names
- Maintain separation of concerns
- Follow the established project structure

## 🎉 Recognition

Contributors will be recognized in:

- **README.md** - List of contributors
- **Release notes** - Feature acknowledgments
- **GitHub contributors** - Automatic recognition

## 📞 Getting Help

If you need help contributing:

1. **Check existing issues** - Your question might already be answered
2. **Review documentation** - Check README and code comments
3. **Create an issue** - For bugs or feature requests
4. **Join discussions** - Participate in existing conversations

## 📄 License

By contributing to Carbon Foodprint Scanner, you agree that your contributions will be licensed under the MIT License.

---

**Thank you for contributing to sustainable consumption awareness! 🌱** 