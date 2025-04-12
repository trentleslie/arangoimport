# Case Studies

This directory contains detailed case studies of real-world migrations to ArangoDB using the ArangoImport tool.

## Available Case Studies

### SPOKE Biomedical Knowledge Graph

The [SPOKE case study](./spoke/) documents the migration of the SPOKE (Scalable Precision Medicine Open Knowledge Engine) biomedical knowledge graph from Neo4j to ArangoDB. It includes:

- [Migration Analysis](./spoke/migration_analysis.md): Detailed analysis of the migration process and challenges
- [Troubleshooting](./spoke/troubleshooting.md): Solutions to common issues encountered during the migration
- [Database Analysis](./spoke/database_analysis.md): Analysis of the database structure and content

## Adding Your Own Case Study

If you've used ArangoImport for your own data migration and would like to contribute a case study, please follow this structure:

```
/docs/case_studies/your_project_name/
├── README.md                # Overview of your case study
├── migration_analysis.md    # Analysis of your migration process
├── troubleshooting.md       # Issues encountered and their solutions
└── database_analysis.md     # Analysis of your database structure
```

Your case study should include:

1. **Project Overview**: Description of your data and use case
2. **Migration Process**: How you exported from the source database and imported to ArangoDB
3. **Challenges and Solutions**: Technical challenges you faced and how you solved them
4. **Performance Metrics**: Size of your data, import times, and performance comparisons
5. **Lessons Learned**: Advice for others attempting similar migrations
