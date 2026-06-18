# Contributing to Orange Money Pipeline

Merci de votre intérêt pour ce projet ! Ce dépôt est maintenu par [XamXam Graph](https://xamxamgraph.com), un studio d'architecture de données basé à Dakar et Montréal.

## Comment contribuer

1. **Fork** le dépôt
2. Créez une branche : `git checkout -b feature/votre-feature`
3. Faites vos modifications en suivant les conventions ci-dessous
4. Assurez-vous que les tests passent : `pytest tests/`
5. Vérifiez le linting : `ruff check src/ tests/`
6. **Push** et ouvrez une Pull Request

## Conventions

- **Python** : Black (line-length=100), Ruff, mypy
- **SQL / dbt** : lowercase keywords, trailing commas, 4-space indentation
- **Commits** : [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `docs:`, `test:`, `refactor:`
- **Documentation** : Docstrings en anglais, commentaires inline en français quand pertinent

## Priorités de contribution

Nous accueillons particulièrement les contributions sur :

1. **Support d'APIs supplémentaires** — Wave, Free Money, MTN Mobile Money, Moov Money
2. **Patterns de détection de fraude additionnels** — ML-based anomaly detection
3. **Intégrations de gouvernance** — Unity Catalog policies, CDP audit trails
4. **Support multi-pays** — UEMOA, CEMAC, autres zones monétaires
5. **Documentation et traductions** — améliorations FR/EN, tutoriels vidéo

## Contact

Pour toute question, contactez-nous à **engineering@xamxamgraph.com** ou ouvrez une issue.
