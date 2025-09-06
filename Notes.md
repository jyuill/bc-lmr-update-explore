# Project Notes

This file is for temporary notes, brainstorming, and tracking next steps.  
When something becomes important or long-term, consider moving it to a GitHub Issue.

---

## 🚀 Next Steps
- [ ] Implement logging for error handling
- [ ] Add unit tests for data validation
- [ ] Refactor `utils.py` into separate modules

---

## 🐛 Bugs / Fixes
- [ ] API call sometimes times out → add retry logic
- [ ] Plot in `analysis.R` breaks with missing data
- [ ] Fix deprecation warning: `label_dollar` → `label_currency`

---

## 💡 Ideas / Improvements
- [ ] Try replacing `pandas` with `polars` for performance
- [ ] Add a Shiny dashboard for visualization
- [ ] Explore using `uv` lockfile instead of requirements.txt freeze

---

## 📋 Notes / Scratchpad
- Remember: remove hardcoded paths before pushing
- Check licensing for external dataset before publishing
- Possible blog post: “Mixing R and Python in Positron with uv + renv”

---

## ✅ Done
- [x] Set up `.venv` with uv
- [x] Added `requirements.txt`
- [x] Configured Positron interpreter to use project env
