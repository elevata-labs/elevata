# NOTICE

This project includes third-party software components that are licensed under their own respective terms.  

---

## Included Components

### Django
- License: [BSD 3-Clause License](https://opensource.org/licenses/BSD-3-Clause)
- Copyright © Django Software Foundation
- Source: [https://www.djangoproject.com/](https://www.djangoproject.com/)

### django-crum
- License: [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html)
- Copyright © 2013–present, Robert Townley and contributors
- Source: [https://github.com/ninemoreminutes/django-crum](https://github.com/ninemoreminutes/django-crum)

### SQLAlchemy
- License: [MIT License](https://opensource.org/licenses/MIT)
- Copyright © 2005–present, SQLAlchemy authors and contributors
- Source: [https://www.sqlalchemy.org/](https://www.sqlalchemy.org/)
- Usage: Database abstraction and metadata inspection

### PyYAML
- License: [MIT License](https://opensource.org/licenses/MIT)
- Copyright © 2006–present, Kirill Simonov and contributors
- Source: [https://pyyaml.org/](https://pyyaml.org/)
- Usage: YAML configuration parsing

### python-dotenv
- License: [MIT License](https://opensource.org/licenses/MIT)
- Copyright © 2014–present, Saurabh Kumar and contributors
- Source: [https://github.com/theskumar/python-dotenv](https://github.com/theskumar/python-dotenv)
- Usage: Environment variable management

### dj-database-url
- License: [BSD 3-Clause License](https://opensource.org/licenses/BSD-3-Clause)
- Copyright © 2012–present, Kenneth Reitz and contributors
- Source: [https://github.com/jacobian/dj-database-url](https://github.com/jacobian/dj-database-url)
- Usage: Parse database URLs for Django settings

### Azure SDK for Python
- Components: `azure-identity`, `azure-keyvault-secrets`
- License: [MIT License](https://opensource.org/licenses/MIT)
- Copyright © Microsoft Corporation
- Source: [https://learn.microsoft.com/en-us/python/api/overview/azure/](https://learn.microsoft.com/en-us/python/api/overview/azure/)
- Usage: Secure secret resolution from Azure Key Vault

### Inter Font
- License: [SIL Open Font License 1.1](https://scripts.sil.org/OFL)
- Copyright © 2016–2024 Rasmus Andersson
- Source: [https://github.com/rsms/inter](https://github.com/rsms/inter)

### Lucide Icons
- Project: Lucide (https://lucide.dev)
- License: [ISC License](https://lucide.dev/license)
- Copyright © 2022 Lucide Contributors
- Source: [https://github.com/lucide-icons/lucide](https://github.com/lucide-icons/lucide)
- Usage: Icon set used in the elevata web interface (navigation and UI elements)

### Other Python Packages
- **pandas** – BSD License  
- **HTMX** – BSD 2-Clause License  
- **Bootstrap 5** – MIT License  
- **pytest** (MIT License) – used for automated testing

### Optional Runtime Dependencies
- **PostgreSQL** – optional database backend  
  (not bundled with this software; distributed under the PostgreSQL License)  
  See [https://www.postgresql.org/about/licence/](https://www.postgresql.org/about/licence/)

---

## License Compatibility

This project as a whole is licensed under the **GNU Affero General Public License v3 (AGPLv3)**.  

- All included third-party components remain under their original licenses.  
- The combination is permitted because each license (BSD, MIT, ISC, OFL, PostgreSQL, Public Domain) is compatible with AGPLv3.  
- elevata’s original source code is governed by AGPLv3.  

---

## Removed Components (as of v0.2.2)

In earlier experimental stages, a placeholder for a `dbt_project/` folder and related configuration variables existed.  
These artefacts have been removed as of version **0.2.2**.  
elevata is now fully independent of dbt or any other external transformation runtime.  

---

## Additional Components

Future features of elevata may integrate further third-party libraries.  
Once included in the codebase, their license information will be documented here.
