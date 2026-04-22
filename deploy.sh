#!/bin/bash
set -e

echo "🚀 M&A QUALIFICATION — AUTOMATED DEPLOYMENT"
echo "==========================================="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}STEP 1: Prepare GitHub${NC}"
echo "This script will push your code to GitHub."
echo "You'll need to authenticate once (browser will open)."
echo ""

# Check if git is configured
if ! git config user.email > /dev/null 2>&1; then
    echo "⚠️  Git not configured. Setting up..."
    read -p "Enter your email: " email
    read -p "Enter your name: " name
    git config --global user.email "$email"
    git config --global user.name "$name"
    echo "✅ Git configured"
fi

echo ""
echo -e "${BLUE}STEP 2: Create GitHub Repository${NC}"
echo "Go to: https://github.com/new"
echo ""
echo "Fill in these fields:"
echo "  Repository name: bus-ma-targets"
echo "  Description: M&A Qualification - Quebec Bus Operators"
echo "  Visibility: Public"
echo "  Add README: Yes"
echo ""
echo "Then click: Create repository"
echo ""
read -p "Press ENTER after creating the repo on GitHub..."

echo ""
echo -e "${BLUE}STEP 3: Add remote and push code${NC}"
read -p "Paste your GitHub repo URL (https://github.com/...): " repo_url

git remote remove origin 2>/dev/null || true
git remote add origin "$repo_url"
git branch -M main
git push -u origin main

echo ""
echo -e "${GREEN}✅ Code pushed to GitHub!${NC}"
echo ""
echo -e "${BLUE}STEP 4: Deploy to Vercel${NC}"
echo "Go to: https://vercel.com/new/import"
echo ""
echo "1. Paste this URL into the import box:"
echo "   $repo_url"
echo ""
echo "2. Click 'Import'"
echo "3. Wait 2-3 minutes for deployment"
echo ""
read -p "Press ENTER after Vercel completes deployment..."

echo ""
echo -e "${GREEN}✅ DEPLOYMENT COMPLETE!${NC}"
echo ""
echo "Your dashboard is now live at:"
echo "👉 https://bus-ma-targets.vercel.app/targets"
echo ""
echo "Share this URL with your client!"
echo ""
echo "Future updates: Just run 'git push origin main' to auto-deploy"

