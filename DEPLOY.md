# How to put APEX online so friends can see it 24/7

You need to **host** the app on a free cloud service. Your API keys go in that service’s dashboard (not in the code), so it stays secure and runs even when your laptop is off.

---

## Option 1: Render.com (easiest – one Python server)

1. **Push your project to GitHub**  
   Create a repo and push the `Myfinance` folder (you can leave `.env` out; add it to `.gitignore` if it isn’t already).

2. **Sign up** at [render.com](https://render.com) (free).

3. **New Web Service**  
   Dashboard → **New +** → **Web Service**.

4. **Connect the repo**  
   Connect your GitHub and select the repo that contains `dashboard.html` and `finance-ai/server.py`.

5. **Settings**
   - **Build command:** leave empty or `pip install -r requirements.txt`
   - **Start command:** `python finance-ai/server.py`
   - **Root directory:** leave empty (repo root)

6. **Environment variables**  
   In the service’s **Environment** tab, add (with your real keys):
   - `ALPHA_VANTAGE_KEY` = your key  
   - `FINNHUB_TOKEN` = your key  
   - `FRED_API_KEY` = your key (optional)

7. **Deploy**  
   Click **Create Web Service**. After a few minutes you’ll get a URL like:
   `https://your-app-name.onrender.com`

8. **Share**  
   Send that link to friends. They open it in a browser and click **REFRESH** to see live data. It keeps updating as long as the app is running (free tier may spin down after ~15 min idle; first open after that can be slow).

**Note:** On Render, the app is served from that URL. The dashboard already uses “same origin” when not on localhost, so it will call `/api/quotes` and `/api/treasury` on that same URL. No extra config needed.

---

## Option 2: Railway.app

1. Sign up at [railway.app](https://railway.app).
2. **New Project** → **Deploy from GitHub** and select your repo.
3. Set **Start command:** `python finance-ai/server.py`.
4. In **Variables**, add `ALPHA_VANTAGE_KEY`, `FINNHUB_TOKEN`, `FRED_API_KEY`.
5. Deploy and use the generated public URL. Share that link with friends.

---

## Summary

| Step | What you do |
|------|----------------------|
| 1 | Put the project on GitHub (without `.env`). |
| 2 | Create a Web Service on Render (or Railway) linked to that repo. |
| 3 | Set the **start command** to `python finance-ai/server.py`. |
| 4 | Add your API keys as **environment variables** in the host’s dashboard. |
| 5 | Deploy and share the public URL. |

Friends only need the link; they don’t install anything. Data stays live as long as the free service is running (and you’re not sharing your keys with anyone).
