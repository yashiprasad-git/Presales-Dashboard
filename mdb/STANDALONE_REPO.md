# Push only `mdb/` to a separate GitHub repo (one source of truth)

Your real code lives in **`presales/mdb/`** inside the main repo. Do **not** maintain a second copy on Desktop unless you want duplicates.

The **`Desktop/presales-mdb`** folder was a one-time export. You can delete it after you use the flow below.

---

## One-time: create empty repo on GitHub

Create a **new empty** private repo, e.g. `presales-mdb` (no README).

---

## First push of the `mdb/` folder

From the **parent** repo (`presales/`, not inside `mdb/`):

```bash
cd ~/Desktop/presales

# Add the new repo as a remote (pick a short name)
git remote add mdb-repo https://github.com/YOUR_USERNAME/presales-mdb.git

# Push only the mdb/ subtree to main on that remote
git subtree split -P mdb -b mdb-export
git push mdb-repo mdb-export:main
```

If `git subtree split` warns about ambiguous commits, use the same commands again after your next commit on `main`.

---

## Later: after you change files under `mdb/`

```bash
cd ~/Desktop/presales
git add mdb/
git commit -m "Your message"
git push origin main

# Push mdb-only updates to the IT repo
git subtree push --prefix=mdb mdb-repo main
```

If `subtree push` fails (known on large repos), use the split + push fallback:

```bash
git subtree split -P mdb -b mdb-export
git push mdb-repo mdb-export:main --force
```

---

## Streamlit / Actions on the IT repo

That remote only contains the contents of `mdb/` at the **root** of the new repo (same layout as today). Point Streamlit and Actions secrets there as documented earlier.

---

## Summary

| Location | Role |
|----------|------|
| `presales/mdb/` | **Edit here** — only place you maintain MDB code |
| `Desktop/presales-mdb/` | Optional duplicate; safe to **delete** if you use subtree |
| Second GitHub repo | What IT sees; updated via `git subtree push` |
