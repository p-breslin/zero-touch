import re
import logging
from dotenv import load_dotenv
from utils.helpers import get_arango_db
from utils.logging_setup import setup_logging


load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

# Collection names
USERS_COLL = "Users"
SKILLS_COLL = "Skills"
EDGE_SKILL_TO_USER = "skill_to_user"


def sanitize_skill_name(skill):
    """
    Convert a skill string into a valid Arango _key:
    - lowercase
    - replace non-alphanumeric characters with '_'
    - strip leading/trailing underscores
    """
    key = re.sub(r"[^0-9a-zA-Z]+", "_", skill.strip().lower())
    return key.strip("_")


def create_and_populate_skills_graph():
    """
    1. Verify that 'Users' collection exists.
    2. Create 'Skills' vertex collection and 'skill_to_user' edge collection if missing.
    3. For each user document, read the 'skills' array.
       For each skill:
         a. Insert (/skip) a doc into 'Skills' with _key = sanitized skill.
         b. Insert (/skip) an edge from Skills/<skill_key> -> Users/<user_uuid>.
    """
    db = get_arango_db()

    # 1. Check that 'Users' collection exists
    if not db.has_collection(USERS_COLL):
        log.error(f"Required collection '{USERS_COLL}' does not exist.")
        return

    # 2. Create 'Skills' vertex collection if missing
    if db.has_collection(SKILLS_COLL):
        log.info(f"Vertex collection '{SKILLS_COLL}' already exists; proceeding.")
    else:
        try:
            db.create_collection(SKILLS_COLL)
            log.info(f"Created vertex collection '{SKILLS_COLL}'.")
        except Exception as e:
            log.error(f"Failed to create vertex collection '{SKILLS_COLL}': {e}")
            return

    #    Create 'skill_to_user' edge collection if missing
    if db.has_collection(EDGE_SKILL_TO_USER):
        log.info(f"Edge collection '{EDGE_SKILL_TO_USER}' already exists; proceeding.")
    else:
        try:
            db.create_collection(EDGE_SKILL_TO_USER, edge=True)
            log.info(f"Created edge collection '{EDGE_SKILL_TO_USER}'.")
        except Exception as e:
            log.error(f"Failed to create edge collection '{EDGE_SKILL_TO_USER}': {e}")
            return

    user_col = db.collection(USERS_COLL)
    skill_col = db.collection(SKILLS_COLL)
    edge_col = db.collection(EDGE_SKILL_TO_USER)

    # 3. Iterate over all users
    for user_doc in user_col.all():
        user_key = user_doc["_key"]
        skills = user_doc.get("skills", [])
        if not isinstance(skills, list):
            log.warning(f"User '{user_key}' has invalid 'skills' field; skipping.")
            continue

        for skill in skills:
            if not skill or not isinstance(skill, str):
                continue

            skill_key = sanitize_skill_name(skill)
            if not skill_key:
                continue

            # 3.a Insert skill vertex if not exists
            if skill_col.has(skill_key):
                log.debug(f"Skill '{skill_key}' already exists; skipping insert.")
            else:
                skill_doc = {"_key": skill_key, "name": skill.strip()}
                try:
                    skill_col.insert(skill_doc)
                    log.info(f"Inserted Skill '{skill_key}'.")
                except Exception as e:
                    log.error(f"Failed to insert Skill '{skill_key}': {e}")
                    continue

            # 3.b Insert edge from Skill -> User
            edge_key = f"{skill_key}-{user_key}"
            if edge_col.has(edge_key):
                log.debug(f"Edge '{edge_key}' already exists; skipping.")
            else:
                edge_doc = {
                    "_key": edge_key,
                    "_from": f"{SKILLS_COLL}/{skill_key}",
                    "_to": f"{USERS_COLL}/{user_key}",
                }
                try:
                    edge_col.insert(edge_doc)
                    log.info(f"Inserted edge '{edge_key}'.")
                except Exception as e:
                    log.error(f"Failed to insert edge '{edge_key}': {e}")


def main():
    create_and_populate_skills_graph()
    log.info("Created and populated the skills cloud.")


if __name__ == "__main__":
    main()
