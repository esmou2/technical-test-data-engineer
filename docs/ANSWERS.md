# Réponses du test

## _Utilisation de la solution (étape 1 à 3)_

j'ai utiliser **venv** pour la gestion de l'environnement virtuel et des dépendances.

---

Créer et activer un environnement virtuel avec Pipenv

```bash
venv\Scripts\activate
```
### 2. Installer les dépendances
Une fois l'environnement virtuel activé, installez toutes les dépendances nécessaires définies dans le fichier requirements.txt.

```bash
pip install -r requirements.txt
```
### 4. Lancer l'API locale
Placez vous dans le dossier `src/moovitamix_fastapi`, puis exécuter dans votre terminal l'instruction suivante `python -m uvicorn main:app --reload`. Vous retrouverz ensuite l'URL pour accéder à l'application en local. L'application vous redirige automatiquement vers le chemin /docs, si ce n'est pas le cas, rendez-vous directement à: <http://127.0.0.1:8000/docs>.

### 5. Configurer et exécuter le pipeline de données
Configurer les variables d'environnement

Le pipeline utilise des variables d'environnement pour l'URL de l'API et le répertoire de stockage des données. Créez un fichier .env à la racine de votre projet avec les informations suivantes :
```bash
# .env file
API_URL=http://127.0.0.1:8000
STORAGE_DIR=data
```

Exécuter le pipeline
Une fois l'API en cours d'exécution et les variables d'environnement configurées, vous pouvez exécuter le pipeline pour récupérer les données :

```bash
python .\src\main.py
```

### 6. Lancer les tests unitaires
Pour exécuter les tests unitaires, utilisez **pytest** avec la commande suivante :

```bash
pytest
```
Vérifier les résultats des tests
Une fois les tests exécutés, vous verrez un résumé des résultats dans la console.

## Questions (étapes 4 à 7)

### Étape 4

Pour stocker les informations provenant des trois sources de données je propose un schéma relationnel en trois tables principales :

Table users :

- `id` (PRIMARY KEY): L'identifiant unique de l'utilisateur
- `first_name`: Prénom de l'utilisateur
- `last_name`: Nom de l'utilisateur
- `email`: Email de l'utilisateur
- `gender`: Genre de l'utilisateur
- `favorite_genres`: Genres musicaux préférés
- `created_at`: Date de création de l'utilisateur
- `updated_at`: Dernière mise à jour
- `charged_at`: Date d'ajout dans la bd

Table tracks :

- `id` (PRIMARY KEY): L'identifiant unique de la chanson
- `name`: Titre de la chanson
- `artist`: Nom de l’artiste
- `songwriters`: Ecrivain de la chanson
- `duration`: Durée de la chanson en secondes
- `genres`:
- `album`
- `created_at`: Date d'ajout de la chanson
- `updated_at`: Dernière mise à jour
- `charged_at`: Date d'ajout dans la bd


Table listen_history :

- `id` (PRIMARY KEY): L’identifiant unique de l'historique d'écoute.
- `user_id` (`FOREIGN KEY` vers `users`): Référence vers l’utilisateur.
- `track_id` (`FOREIGN KEY` ver `tracks`): Référence vers la chanson écoutée.
- `timestamp`: Date et heure où la chanson a été écoutée.
- `created_at`: Date de création de cet enregistrement.
- `updated_at`: Dernière mise à jour.
- `charged_at`: Date d'ajout dans la bd.

Je recommande l’utilisation d’une base de données relationnelle pour ce type de projet. pour les raisons suivantes:
- Les relations entre utilisateurs, chansons et historique d'écoute impliquent souvent des jointures, ce qui est efficacement géré par une base de données relationnelle.
- Les données ont un format structuré et stable, rendant une base relationnelle appropriée.
- Il est possible de gérer un nombre croissant d’utilisateurs, de chansons, et de l’historique d’écoute grâce à des stratégies d'indexation et de partitionnement.
- Ce schéma permet de répondre efficacement à des requêtes complexes, comme celles nécessaires pour un modèle de recommandation.

### Étape 5
Pour suivre la santé du pipeline de données dans son exécution quotidienne, j'ai mis en place
- Chronométrage du pipeline: Mesure du temps d'exécution global pour identifier les goulets d'étranglement.
- Gestion des erreurs: Log des erreurs rencontrées à chaque étape.
Enregistrement des codes d'erreur et des messages pour faciliter le débogage.
- Logs des étapes: Suivi détaillé de chaque étape du pipeline (initialisation, récupération, sauvegarde).
- Suivi des données: Logging de la quantité de données récupérées et persister.

Pour renforcer la fiabilité du pipeline de données, je propose de mettre en place une solution d'alerte qui fonctionnera comme suit:
- Envoi d'alertes par email aux membres de l'équipe en cas d'échec critique du pipeline
- Utilisation de services comme Slack ou Microsoft Teams pour envoyer des notifications instantanées
- Utiliser un outile comme Amazon CloudWatch pour créer des métriques personnalisées, visualiser les performances dans un tableau de bord, et configurer des alertes automatiques en cas d'échec ou d'anomalie dans l'exécution du pipeline.
- Mettre en place une logique de retry pour les appels à l'API serait également une solution efficace pour améliorer la robustesse du pipeline de données.

### Étape 6
Étapes:
- Utiliser le pipeline de données existant pour ingérer quotidiennement les données des utilisateurs et leurs historiques d'écoute.
- Prétraiter les données pour s'assurer qu'elles sont au bon format (nettoyage, normalisation, etc.).
- Intégrer le modèle de recommandation dans le pipeline, soit via une API, soit en intégrant directement le code du modèle, ou en le déployant dans un outil tel que SageMaker pour effectuer des inférences en temps réel.
- Créer un script qui appelle le modèle de recommandation avec les données prétraitées pour générer les recommandations.
- Planifier l'exécution du script pour qu'il se lance automatiquement après l'ingestion quotidienne des données.
- Stocker les recommandations générées dans une base de données.
- Mettre en place une surveillance pour évaluer la performance du modèle de recommandation et détecter tout changement au fil du temps.
### Étape 7
Pour automatiser le réentrainement:
- Suivre des métriques clés comme la précision et le rappel en utilisant des outils de monitoring (cloudwatch).
- Établir des seuils de performance qui déclencheront le réentraînement.
- Utiliser des scripts et des outils de planification pour automatiser cette vérification (stepfunction, Cron).
- Lorsque la dérive de performance est détectée, exécuter un script qui prépare et lance le réentraînement du modèle (Sagemaker). 
- Dans le script, charger les nouvelles données, prétraiter ces données, puis entraîner le modèle.
- Déployer le modèle réentraîné dans l'environnement de production tout en gardant l'ancienne version.
- Exécuter des tests automatisés pour s'assurer que le nouveau modèle fonctionne correctement.

