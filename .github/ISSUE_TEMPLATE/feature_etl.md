name: Feature: ETL Tool Development
description: Propose a new feature or enhancement for the ETL tool
title: "[Feature] <Short descriptive title>"
labels: [feature, etl]

body:
  - type: input
    id: name
    attributes:
      label: Feature Name
      placeholder: "e.g., CSV Import Module"

  - type: textarea
    id: description
    attributes:
      label: Description
      description: What should be implemented and why?
      placeholder: |
        Describe the purpose and goals of the feature. Include relevant technical or business context.

  - type: checkboxes
    id: todos
    attributes:
      label: Tasks
      description: Key steps to implement this feature
      options:
        - label: Define architecture or data flow
        - label: Analyze data source
        - label: Implement extraction logic
        - label: Define and test transformation
        - label: Integrate loading process
        - label: Write tests
        - label: Perform code review
        - label: Update documentation

  - type: input
    id: contact
    attributes:
      label: Point of Contact
      placeholder: "@username or relevant team"

  - type: textarea
    id: resources
    attributes:
      label: Additional Information
      placeholder: |
        Links to specs, design docs, or related tickets

  - type: textarea
    id: criteria
    attributes:
      label: Acceptance Criteria
      placeholder: |
        - Data is correctly loaded and transformed
        - No runtime errors or data loss
        - Tests pass
