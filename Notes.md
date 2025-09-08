# Project Notes

This file is for temporary notes, brainstorming, and tracking next steps.  
When something becomes important or long-term, consider moving it to a GitHub Issue.

See also Asana project: [Data Products/Portfolio](https://app.asana.com/1/1153963424016572/project/1199532265464339/list/1205106600548958)
---

## 🚀 Next Steps
- [ ] add credentials for PostgreSQL RDS instance on AWS and test connection
- [ ] update functions/lmr_db_functions.R to use new PostgreSQL RDS instance
- [ ] setup Claude-code-notes.md with prompt notes for PDF parsing system
- [ ] create new system for parsing data from LMR PDFs
- [ ] make sure it works for all category tables
    - [ ] sort out issues with Spirits and Wine data -> many NAs, possible misaligned rows, parsing issues
- [ ] finalize data update process documentation

---

## 🐛 Bugs / Fixes
- [ ] fix these

---

## 💡 Ideas / Improvements
- [ ] Try replacing `pandas` with `polars` for performance
- [ ] Explore using `uv` lockfile instead of requirements.txt freeze

---

## 📋 Notes / Scratchpad
- Possible blog post: “Mixing R and Python in Positron with uv + renv”

---

## ✅ Done
- [x] update README.md to reflect new repo name
