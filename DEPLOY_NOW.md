# Get your shareable URL in ~5 minutes

## 1. Put the project on GitHub

- Go to [github.com](https://github.com) and sign in.
- Click **+** → **New repository**.
- Name it (e.g. `apex-dashboard`), leave it empty, click **Create repository**.
- On your Mac, in Terminal:

```bash
cd /Users/eashankarnani/Documents/Myfinance
git init
git add .
git commit -m "APEX dashboard"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
git push -u origin main
```

(Replace `YOUR_USERNAME` and `YOUR_REPO_NAME` with your GitHub username and repo name.)

## 2. Deploy on Render

- Go to [render.com](https://render.com) and sign up (free) with GitHub.
- In the dashboard click **New +** → **Blueprint**.
- Click **Connect account** and choose your GitHub account, then select the repo you just pushed.
- Click **Connect**. Render will read `render.yaml` and show one service: `apex-dashboard`. Click **Apply**.
- Open the new **Web Service** (e.g. `apex-dashboard`).
- Go to the **Environment** tab. Add these variables (paste your real keys):

  | Key | Value |
  |-----|--------|
  | `ALPHA_VANTAGE_KEY` | (your key) |
  | `FINNHUB_TOKEN` | (your key) |
  | `FRED_API_KEY` | (your key, optional) |

- Save. Render will redeploy. Wait 1–2 minutes.

## 3. Get your URL

- In the service page, at the top you’ll see **Your service is live at** → something like:
  **https://apex-dashboard-xxxx.onrender.com**
- That’s your link. Open it, click **REFRESH** in the dashboard, and share the same link with friends.

Nobody can see your API keys; they stay in Render’s environment and never go to the browser.
