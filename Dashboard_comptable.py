"""
Dashboard Comptable PME Agro-alimentaire v2
Navigation corrigee + Demo fichiers Excel reels
pip install dash dash-bootstrap-components plotly pandas openpyxl numpy
python dashboard_comptable.py  ->  http://localhost:8050
"""

import base64, io, os
from datetime import datetime, timedelta

import dash
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, State, dash_table, callback_context, no_update

APP_TITLE  = "ComptaBoard"
DEVISE     = "FCFA"
ENTREPRISE = "AGRO PME COTE D'IVOIRE"

C = {
    "bg":"#0D1117","surface":"#161B22","surface2":"#21262D",
    "border":"#30363D","accent":"#58A6FF","accent5":"#D2A8FF",
    "text":"#E6EDF3","muted":"#7D8590","credit":"#3FB950",
    "debit":"#FF7B72","warning":"#D29922","success":"#3FB950",
}

CATEGORIES = {
    "Matieres premieres":["cacao","cafe","anacarde","noix","riz","mais","farine","sucre","huile","intrant","semence","engrais"],
    "Emballages":["sac","carton","bouteille","bidon","sachet","emballage","etiquette"],
    "Transport":["transport","livraison","camion","fret","transit","douane","carburant","essence"],
    "Energie":["electricite","cie","sodeci","eau","gaz","groupe","generateur","fuel"],
    "Personnel":["salaire","paie","cnps","rts","prime","conge","formation","personnel"],
    "Equipements":["machine","equipement","materiel","reparation","maintenance","outil"],
    "Charges fin.":["interet","agios","commission","frais bancaire","credit","leasing"],
    "Impots":["dgi","impot","taxe","tva","bic","patente","timbre","tresor"],
    "Ventes":["vente","facture","reglement client","acompte","export","encaissement","reglt"],
    "Loyers":["loyer","bail","location","entrepot","bureau","usine"],
    "Telecoms":["mtn","orange","moov","wave","mobile money","telephone","internet"],
    "Assurances":["assurance","saar","nsia","allianz","sinistre"],
}

PLOTLY_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Courier New,monospace",color="#E6EDF3",size=11),
    margin=dict(l=14,r=14,t=36,b=14),
    legend=dict(bgcolor="rgba(0,0,0,0)",bordercolor="#30363D"),
    xaxis=dict(gridcolor="#30363D",linecolor="#30363D"),
    yaxis=dict(gridcolor="#30363D",linecolor="#30363D"),
)

def fmt_m(v):
    v=float(v)
    if abs(v)>=1_000_000: return f"{v/1_000_000:.1f}M"
    if abs(v)>=1_000: return f"{v/1_000:.0f}K"
    return f"{v:,.0f}"

def categoriser(lib):
    lb=str(lib).lower()
    for cat,mots in CATEGORIES.items():
        if any(m in lb for m in mots): return cat
    return "Divers"

def rapprocher(df_bq, df_cpt):
    df_bq=df_bq.copy(); df_bq["rapproche"]=False
    df_cpt=df_cpt.copy(); df_cpt["rapproche"]=False
    paires=[]; used=set()
    for i,rb in df_bq.iterrows():
        best_score,best_j=0,None
        for j,rc in df_cpt.iterrows():
            if j in used: continue
            if abs(rb["montant"]-rc["montant"])>1: continue
            ecart_j=abs((rb["date"]-rc["date"]).days)
            if ecart_j>5: continue
            score=100-ecart_j*10
            if score>best_score: best_score,best_j=score,j
        if best_j is not None:
            df_bq.at[i,"rapproche"]=True
            df_cpt.at[best_j,"rapproche"]=True
            used.add(best_j)
            paires.append({
                "Date Banque":rb["date"].strftime("%d/%m/%Y"),
                "Libelle Banque":str(rb["libelle"])[:38],
                "Date Compta":df_cpt.at[best_j,"date"].strftime("%d/%m/%Y"),
                "Libelle Compta":str(df_cpt.at[best_j,"libelle"])[:38],
                "Montant":rb["montant"],
                "Ecart jours":abs((rb["date"]-df_cpt.at[best_j,"date"]).days),
                "Score":best_score,
                "Statut":"Rapproche",
            })
    non_bq=df_bq[~df_bq["rapproche"]].copy()
    non_cpt=df_cpt[~df_cpt["rapproche"]].copy()
    return pd.DataFrame(paires),non_bq,non_cpt

SCRIPT_DIR=os.path.dirname(os.path.abspath(__file__))

def charger_releve_excel(chemin):
    try:
        df=pd.read_excel(chemin,skiprows=8)
        df.columns=["date_op","date_val","n_op","libelle","ref","debit","credit","solde"]
        df=df.dropna(subset=["date_op"])
        df=df[df["date_op"].astype(str).str.match(r"\d{2}/\d{2}/\d{4}")]
        df["date"]=pd.to_datetime(df["date_op"],format="%d/%m/%Y",errors="coerce")
        df["debit"]=pd.to_numeric(df["debit"],errors="coerce").fillna(0)
        df["credit"]=pd.to_numeric(df["credit"],errors="coerce").fillna(0)
        df["solde"]=pd.to_numeric(df["solde"],errors="coerce").fillna(0)
        df["montant"]=df["credit"]-df["debit"]
        df=df.dropna(subset=["date"])
        df["categorie"]=df["libelle"].apply(categoriser)
        df["statut"]="A rapprocher"
        df["id"]=[f"BQ{i+1:04d}" for i in range(len(df))]
        df["source"]="Banque"
        return df.reset_index(drop=True)
    except: return None

def charger_export_sage(chemin):
    try:
        df=pd.read_excel(chemin,skiprows=8)
        df.columns=["date","piece","compte","intitule","libelle","ref","journal","debit","credit","statut_sage"]
        df=df.dropna(subset=["date"])
        df=df[df["date"].astype(str).str.match(r"\d{2}/\d{2}/\d{4}")]
        df["date"]=pd.to_datetime(df["date"],format="%d/%m/%Y",errors="coerce")
        df["debit"]=pd.to_numeric(df["debit"],errors="coerce").fillna(0)
        df["credit"]=pd.to_numeric(df["credit"],errors="coerce").fillna(0)
        df["montant"]=df["credit"]-df["debit"]
        df=df.dropna(subset=["date"])
        df["categorie"]=df["libelle"].apply(categoriser)
        df["id"]=[f"CPT{i+1:04d}" for i in range(len(df))]
        df["source"]="Compta"
        return df.reset_index(drop=True)
    except: return None

def demo_banque():
    rows,solde=[],12_450_000
    data=[
        ("VIREMENT RECU GRANDS MOULINS CI",0,2_800_000,"GMC-FAC-0312",2),
        ("PAIEMENT FOURNISSEUR CACAO GHANA LTD",1_500_000,0,"FA2025-018",3),
        ("SALAIRES PERSONNEL MARS 2025",3_200_000,0,"PAIE-MAR25",4),
        ("REGLEMENT CLIENT ABIDJAN FOOD SA",0,1_200_000,"ABF-0156",5),
        ("PAIEMENT SODECI EAU USINE",185_000,0,"SOD-032025",6),
        ("ACHAT EMBALLAGES PLASTIQUES SCI",350_000,0,"FAC-EMB-441",9),
        ("VIREMENT EXPORT NOIX CAJOU EUROPE",0,5_400_000,"EXP-NJ-0089",10),
        ("TRANSPORT LIVRAISON BOUAKE-ABIDJAN",420_000,0,"TRP-2503-07",11),
        ("LOYER ENTREPOT ZONE INDUSTRIELLE PK24",800_000,0,"BAIL-T1-25",12),
        ("REGLEMENT FACTURE SOCOCE SUPERMARCHE",0,950_000,"SOC-FAC-289",13),
        ("CARBURANT VEHICULES TOTAL CI MARS",210_000,0,"TOTAL-032025",16),
        ("MAINTENANCE MACHINE ENSACHAGE SODIMAS",480_000,0,"MAI-2503-02",17),
        ("PRIME ASSURANCE FLOTTE NSIA CI",320_000,0,"NSIA-POL-887",18),
        ("VIREMENT RECU CLIENT GMS MARKET",0,1_750_000,"GMS-0445",19),
        ("ACHAT ANACARDE COOPERATIVE KORHOGO",2_100_000,0,"COOP-KHG-12",20),
        ("FRAIS BANCAIRES ET AGIOS MARS 2025",45_000,0,"FB-032025",23),
        ("REGLEMENT FACTURE RESTAURANT CHAINS CI",0,680_000,"RCI-INV-78",24),
        ("TVA DGI VERSEMENT MENSUEL MARS",890_000,0,"DGI-TVA-03/25",25),
        ("ABONNEMENT ORANGE INTERNET FIBRE PROF",75_000,0,"ORG-ABN-032",26),
        ("FORMATION PERSONNEL HACCP CERTIFICATION",150_000,0,"FORM-2503",27),
        ("REGLEMENT CLIENT EXPORT CACAO EUROPE",0,3_200_000,"EXP-CAC-041",30),
        ("ACHAT FARINE BLANCHE MOULIN MODERNE CI",680_000,0,"MMC-FAC-228",30),
    ]
    base=datetime(2025,3,1)
    for i,(lib,dbt,crd,ref,day) in enumerate(data):
        solde+=crd-dbt
        rows.append({"id":f"BQ{i+1:04d}","date":base+timedelta(days=day),
                     "libelle":lib,"debit":dbt,"credit":crd,"montant":crd-dbt,
                     "solde":solde,"categorie":categoriser(lib),
                     "statut":"A rapprocher","ref":ref,"source":"Banque"})
    return pd.DataFrame(rows)

def demo_compta():
    rows=[]
    data=[
        ("2025-03-03","BQ250301","REGLEMENT FACT. GRANDS MOULINS CI","GMC-FAC-0312",0,2_800_000),
        ("2025-03-04","BQ250302","ACHAT CACAO GHANA LTD FA2025-018","FA2025-018",1_500_000,0),
        ("2025-03-05","BQ250303","PAIEMENT SALAIRES PERSONNEL MARS","PAIE-MAR25",3_200_000,0),
        ("2025-03-06","BQ250304","ENCAISSEMENT FACTURE ABF-0156","ABF-0156",0,1_200_000),
        ("2025-03-07","BQ250305","FACTURE SODECI MARS USINE","SOD-032025",185_000,0),
        ("2025-03-10","BQ250306","EMBALLAGES PLASTIQUES SCI FAC 441","FAC-EMB-441",350_000,0),
        ("2025-03-11","BQ250307","ENCAISSEMENT EXPORT NOIX CAJOU EU","EXP-NJ-0089",0,5_400_000),
        ("2025-03-13","BQ250308","TRANSPORT LIVRAISON BOUAKE MARS","TRP-2503-07",420_000,0),
        ("2025-03-14","BQ250309","LOYER ENTREPOT ZI PK24 T1-2025","BAIL-T1-25",800_000,0),
        ("2025-03-14","BQ250310","REGLT CLIENT SOCOCE FAC 289","SOC-FAC-289",0,950_000),
        ("2025-03-17","BQ250311","CARBURANT FLOTTE TOTAL CI 03/2025","TOTAL-032025",210_000,0),
        ("2025-03-18","BQ250312","MAINTENANCE MACHINE ENSACHAGE","MAI-2503-02",480_000,0),
        ("2025-03-19","BQ250313","ASSURANCE FLOTTE NSIA POLICES 887","NSIA-POL-887",320_000,0),
        ("2025-03-20","BQ250314","ENCAISSEMENT GMS-0445","GMS-0445",0,1_750_000),
        ("2025-03-21","BQ250315","ANACARDE COOPERATIVE KORHOGO","COOP-KHG-12",2_100_000,0),
        ("2025-03-24","BQ250316","FRAIS ET COMMISSIONS BANCAIRES","FB-032025",45_000,0),
        ("2025-03-25","BQ250317","PAIEMENT FACTURE RCI-INV-78","RCI-INV-78",0,680_000),
        ("2025-03-26","BQ250318","VERSEMENT TVA MARS 2025 DGI","DGI-TVA-03/25",890_000,0),
        ("2025-03-27","BQ250319","ORANGE INTERNET PROFESSIONNEL","ORG-ABN-032",75_000,0),
        ("2025-03-31","BQ250321","ENCAISSEMENT EXPORT CACAO EU","EXP-CAC-041",0,3_200_000),
        ("2025-03-31","BQ250322","FARINE BLANCHE MOULIN MODERNE CI","MMC-FAC-228",680_000,0),
        ("2025-03-31","OD250301","AMORTISSEMENT MATERIELS MARS 2025","OD-AM-0325",425_000,0),
    ]
    for i,(dt,piece,lib,ref,dbt,crd) in enumerate(data):
        rows.append({"id":f"CPT{i+1:04d}","date":pd.Timestamp(dt),
                     "libelle":lib,"debit":dbt,"credit":crd,"montant":crd-dbt,
                     "categorie":categoriser(lib),"ref":ref,"piece":piece,"source":"Compta"})
    return pd.DataFrame(rows)

_f_bq=os.path.join(SCRIPT_DIR,"releve_bancaire_mars_2025.xlsx")
_f_cpt=os.path.join(SCRIPT_DIR,"export_comptable_sage_mars_2025.xlsx")
DF_BQ=charger_releve_excel(_f_bq) if os.path.exists(_f_bq) else None
DF_CPT=charger_export_sage(_f_cpt) if os.path.exists(_f_cpt) else None
if DF_BQ is None: DF_BQ=demo_banque(); SOURCE_BQ="Demo integree"
else: SOURCE_BQ=os.path.basename(_f_bq)
if DF_CPT is None: DF_CPT=demo_compta(); SOURCE_CPT="Demo integree"
else: SOURCE_CPT=os.path.basename(_f_cpt)

DF_RAPPR,DF_NON_BQ,DF_NON_CPT=rapprocher(DF_BQ,DF_CPT)
TAUX_RAPPR=len(DF_RAPPR)/max(len(DF_BQ),1)*100

CSS="""
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@400;500;600&display=swap');
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:#0D1117;color:#E6EDF3;font-family:'Inter',sans-serif;font-size:13px;-webkit-font-smoothing:antialiased}
.app-shell{display:flex;height:100vh;overflow:hidden}
.sidebar{width:220px;min-width:220px;background:#161B22;border-right:1px solid #30363D;display:flex;flex-direction:column;overflow-y:auto}
.main-area{flex:1;overflow-y:auto;background:#0D1117;display:flex;flex-direction:column}
.sidebar-logo{display:flex;align-items:center;gap:12px;padding:20px 16px;border-bottom:1px solid #30363D}
.logo-mark{width:36px;height:36px;background:linear-gradient(135deg,#58A6FF,#D2A8FF);border-radius:8px;display:flex;align-items:center;justify-content:center;font-family:'JetBrains Mono',monospace;font-weight:700;font-size:14px;color:white;flex-shrink:0}
.logo-title{font-weight:700;font-size:15px;color:#E6EDF3;font-family:'JetBrains Mono',monospace}
.logo-sub{font-size:9px;color:#7D8590;letter-spacing:.3px;margin-top:1px}
.sidebar-nav{padding:12px 8px;flex:1}
.nav-item{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:6px;cursor:pointer;transition:background .15s,color .15s;color:#7D8590;font-size:13px;font-weight:500;margin-bottom:2px;user-select:none;border:none;width:100%;background:transparent;text-align:left}
.nav-item:hover{background:#21262D;color:#E6EDF3}
.nav-active{background:rgba(88,166,255,.13) !important;color:#58A6FF !important;font-weight:600 !important}
.sidebar-divider{height:1px;background:#30363D;margin:4px 0}
.sidebar-status{padding:12px 16px}
.top-header{background:#161B22;border-bottom:1px solid #30363D;padding:14px 24px;display:flex;align-items:center;justify-content:space-between;min-height:56px}
.header-title{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:16px}
.header-actions{display:flex;align-items:center;gap:8px}
.hbadge{padding:4px 10px;border-radius:20px;font-size:10px;font-family:'JetBrains Mono',monospace;font-weight:600;letter-spacing:.5px;border:1px solid #30363D;color:#7D8590}
.page-title{font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:700;color:#E6EDF3;margin-bottom:18px;letter-spacing:-.3px}
.card-title{font-family:'JetBrains Mono',monospace;font-size:11px;font-weight:600;color:#7D8590;text-transform:uppercase;letter-spacing:.8px;padding:14px 16px 0}
.kpi-grid{display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap}
.kpi-card{flex:1;min-width:155px;background:#161B22;border:1px solid #30363D;border-radius:8px;padding:14px 16px;transition:border-color .2s,transform .15s}
.kpi-card:hover{border-color:rgba(88,166,255,.3);transform:translateY(-1px)}
.kpi-inner{display:flex;align-items:flex-start;gap:10px}
.kpi-icon{font-size:20px;margin-top:2px}
.kpi-label{font-size:10px;color:#7D8590;text-transform:uppercase;letter-spacing:.8px;margin-bottom:4px}
.kpi-value{font-family:'JetBrains Mono',monospace;font-size:22px;font-weight:700;line-height:1.1}
.kpi-sub{font-size:10px;color:#7D8590;margin-top:3px}
.charts-row{display:flex;gap:12px;margin-bottom:14px;flex-wrap:wrap}
.chart-card{background:#161B22;border:1px solid #30363D;border-radius:8px;overflow:hidden;min-width:240px}
.f1{flex:1}.f2{flex:2}.f3{flex:3}
.txn-list{padding:4px 0}
.txn-row{display:flex;justify-content:space-between;align-items:center;padding:8px 16px;border-bottom:1px solid rgba(48,54,61,.5);transition:background .1s}
.txn-row:hover{background:#21262D}
.txn-lib{font-size:12px;font-weight:500;margin-bottom:2px}
.txn-date{font-size:10px;color:#7D8590;font-family:monospace}
.txn-right{text-align:right;display:flex;flex-direction:column;align-items:flex-end;gap:4px}
.txn-mt{font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:700}
.filter-bar{display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;background:#161B22;border:1px solid #30363D;border-radius:8px;padding:14px 16px;margin-bottom:14px}
.filter-group{display:flex;flex-direction:column;gap:4px;min-width:130px}
.flabel{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.7px;color:#7D8590}
.srch{background:#21262D;border:1px solid #30363D;border-radius:6px;color:#E6EDF3;padding:7px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;outline:none;transition:border-color .2s;width:100%}
.srch:focus{border-color:#58A6FF}
.table-wrapper{background:#161B22;border:1px solid #30363D;border-radius:8px;overflow:hidden;margin-bottom:12px}
.table-count{padding:9px 16px;font-size:11px;color:#7D8590;font-family:'JetBrains Mono',monospace;border-bottom:1px solid #30363D}
.action-bar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:12px 0;margin-top:4px}
.btn{padding:8px 16px;border-radius:6px;cursor:pointer;font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:600;border:1px solid transparent;transition:all .15s;white-space:nowrap}
.btn-g{background:rgba(63,185,80,.12);color:#3FB950;border-color:rgba(63,185,80,.27)}
.btn-g:hover{background:rgba(63,185,80,.22);border-color:#3FB950}
.btn-b{background:rgba(88,166,255,.12);color:#58A6FF;border-color:rgba(88,166,255,.27)}
.btn-b:hover{background:rgba(88,166,255,.22);border-color:#58A6FF}
.btn-o{background:rgba(210,153,34,.12);color:#D29922;border-color:rgba(210,153,34,.27)}
.btn-o:hover{background:rgba(210,153,34,.22);border-color:#D29922}
.afb{font-family:'JetBrains Mono',monospace;font-size:11px;color:#3FB950}
.rappr-stat{display:flex;justify-content:space-around;margin-top:16px}
.rappr-num{font-family:'JetBrains Mono',monospace;font-size:26px;font-weight:700;text-align:center}
.rappr-lbl{font-size:10px;color:#7D8590;text-align:center;margin-top:2px;text-transform:uppercase;letter-spacing:.5px}
.prog-bar{height:8px;background:#21262D;border-radius:4px;margin:16px;overflow:hidden}
.prog-fill{height:100%;border-radius:4px;transition:width .8s ease}
.src-tag{display:inline-block;background:rgba(88,166,255,.1);color:#58A6FF;border:1px solid rgba(88,166,255,.2);border-radius:4px;padding:2px 8px;font-family:monospace;font-size:10px;margin-left:8px;vertical-align:middle}
.param-section{background:#161B22;border:1px solid #30363D;border-radius:8px;margin-bottom:14px;overflow:hidden}
.param-row{padding:12px 16px;border-bottom:1px solid rgba(48,54,61,.4);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
.param-lbl{font-size:12px;color:#E6EDF3;font-weight:500}
.param-sub{font-size:10px;color:#7D8590;margin-top:2px}
.upload-zone{border:2px dashed #30363D;border-radius:8px;padding:32px 20px;text-align:center;cursor:pointer;background:#21262D;transition:border-color .2s;margin:16px}
.upload-zone:hover{border-color:#58A6FF}
.Select-control{background:#21262D !important;border:1px solid #30363D !important;border-radius:6px !important;min-height:34px !important}
.Select-value-label,.Select-placeholder{color:#E6EDF3 !important;font-size:12px !important}
.Select-menu-outer{background:#21262D !important;border:1px solid #30363D !important;z-index:999}
.VirtualizedSelectOption{color:#E6EDF3 !important;font-size:12px !important}
.VirtualizedSelectFocusedOption{background:#161B22 !important}
.Select-arrow{border-top-color:#7D8590 !important}
.radio-grp label{color:#7D8590;font-size:12px;margin-right:14px;cursor:pointer}
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:#161B22}
::-webkit-scrollbar-thumb{background:#30363D;border-radius:3px}
"""

def kpi(titre,valeur,sub="",color=None,icon=""):
    color=color or "#58A6FF"
    return html.Div([html.Div([
        html.Span(icon,className="kpi-icon"),
        html.Div([html.Div(titre,className="kpi-label"),
                  html.Div(valeur,className="kpi-value",style={"color":color}),
                  html.Div(sub,className="kpi-sub") if sub else None],
                 style={"flex":"1"}),
    ],className="kpi-inner")],className="kpi-card")

def tbl_hdr(bg="#2C3E50"):
    return {
        "style_table":{"overflowX":"auto"},
        "style_header":{"backgroundColor":bg,"color":"#FFFFFF","fontFamily":"Courier New,monospace",
                        "fontSize":"10px","fontWeight":"700","border":"1px solid #30363D",
                        "padding":"9px 12px","textTransform":"uppercase","letterSpacing":".5px"},
        "style_cell":{"backgroundColor":"#161B22","color":"#E6EDF3","fontFamily":"Courier New,monospace",
                      "fontSize":"11px","border":"1px solid #30363D","padding":"8px 12px",
                      "textOverflow":"ellipsis","maxWidth":"220px"},
        "style_data_conditional":[{"if":{"row_index":"odd"},"backgroundColor":"#21262D"}],
    }

# ── PAGE DASHBOARD ──────────────────────────────────────────────
def build_dashboard():
    df=DF_BQ; total_cr=df["credit"].sum(); total_db=df["debit"].sum()
    solde=total_cr-total_db; nb_r=len(DF_RAPPR); taux=TAUX_RAPPR
    df_s=df.sort_values("date")
    fig_s=go.Figure()
    fig_s.add_trace(go.Scatter(x=df_s["date"],y=df_s["solde"],mode="lines",
        line=dict(color="#58A6FF",width=2.5),fill="tozeroy",fillcolor="rgba(88,166,255,.08)",
        hovertemplate="<b>%{x|%d/%m/%Y}</b><br>%{y:,.0f} FCFA<extra></extra>"))
    fig_s.update_layout(**PLOTLY_BASE,height=200,
        title=dict(text="Evolution du solde",font=dict(size=12,color="#7D8590")))
    df2=df.copy(); df2["mois"]=df2["date"].dt.to_period("M").astype(str)
    flux=df2.groupby("mois").agg(debits=("debit","sum"),credits=("credit","sum")).reset_index()
    fig_f=go.Figure()
    fig_f.add_trace(go.Bar(x=flux["mois"],y=flux["credits"],name="Credits",marker_color="#3FB950",marker_line_width=0))
    fig_f.add_trace(go.Bar(x=flux["mois"],y=flux["debits"],name="Debits",marker_color="#FF7B72",marker_line_width=0))
    fig_f.update_layout(**PLOTLY_BASE,barmode="group",height=200,
        title=dict(text="Flux credit / debit",font=dict(size=12,color="#7D8590")))
    cat_db=df[df["debit"]>0].groupby("categorie")["debit"].sum().nlargest(8)
    pal=["#58A6FF","#3FB950","#F0883E","#FF7B72","#D2A8FF","#D29922","#79C0FF","#56D364"]
    fig_c=go.Figure(go.Pie(labels=cat_db.index,values=cat_db.values,hole=.62,
        marker=dict(colors=pal,line=dict(color="#0D1117",width=2)),textinfo="percent",
        hovertemplate="<b>%{label}</b><br>%{value:,.0f} FCFA<extra></extra>"))
    fig_c.add_annotation(text=f"<b>{fmt_m(total_db)}</b>",x=.5,y=.5,
        showarrow=False,font=dict(size=15,color="#E6EDF3"))
    fig_c.update_layout(**{**PLOTLY_BASE,"legend":dict(bgcolor="rgba(0,0,0,0)",bordercolor="#30363D",font=dict(size=8))},height=260,showlegend=True,
        title=dict(text="Depenses par categorie",font=dict(size=12,color="#7D8590")))
    recent=df.sort_values("date",ascending=False).head(7)
    rows_txn=[]
    for _,r in recent.iterrows():
        col="#3FB950" if r["montant"]>0 else "#FF7B72"
        sg="+" if r["montant"]>0 else ""
        rows_txn.append(html.Div([
            html.Div([html.Div(str(r["libelle"])[:34]+"..." if len(str(r["libelle"]))>34 else str(r["libelle"]),className="txn-lib"),
                      html.Div(r["date"].strftime("%d/%m/%Y"),className="txn-date")]),
            html.Div([html.Div(f"{sg}{fmt_m(r['montant'])}",className="txn-mt",style={"color":col}),
                      html.Span(r.get("statut","—"),style={"fontSize":"10px","padding":"1px 6px",
                               "borderRadius":"4px","fontFamily":"monospace","fontWeight":"600",
                               "background":"rgba(63,185,80,.15)","color":"#3FB950"})],
                     className="txn-right")],className="txn-row"))
    return html.Div([
        html.Div([html.Div("Tableau de bord",className="page-title",style={"display":"inline"}),
                  html.Span(SOURCE_BQ,className="src-tag")],style={"marginBottom":"18px"}),
        html.Div([kpi("Solde Net",fmt_m(solde),"Tresorerie","#3FB950" if solde>=0 else "#FF7B72","💰"),
                  kpi("Credits",fmt_m(total_cr),f"{(df['credit']>0).sum()} operations","#3FB950","📈"),
                  kpi("Debits",fmt_m(total_db),f"{(df['debit']>0).sum()} operations","#FF7B72","📉"),
                  kpi("Rapprochement",f"{taux:.1f}%",f"{nb_r}/{len(df)} tx",
                      "#3FB950" if taux>80 else "#D29922","🔗"),
                  kpi("Ecarts",str(len(DF_NON_BQ)+len(DF_NON_CPT)),"A traiter",
                      "#FF7B72" if (len(DF_NON_BQ)+len(DF_NON_CPT))>0 else "#3FB950","⚠️")],
                 className="kpi-grid"),
        html.Div([html.Div([dcc.Graph(figure=fig_s,config={"displayModeBar":False})],className="chart-card f2"),
                  html.Div([dcc.Graph(figure=fig_f,config={"displayModeBar":False})],className="chart-card f2")],
                 className="charts-row"),
        html.Div([html.Div([dcc.Graph(figure=fig_c,config={"displayModeBar":False})],className="chart-card f1"),
                  html.Div([html.Div("Dernieres operations",className="card-title"),
                             html.Div(rows_txn,className="txn-list")],className="chart-card f2")],
                 className="charts-row"),
    ])

# ── PAGE TRANSACTIONS ───────────────────────────────────────────
def build_transactions():
    df=DF_BQ.copy()
    df["date_str"]=df["date"].dt.strftime("%d/%m/%Y")
    df["debit_fmt"]=df["debit"].apply(lambda x:f"{x:,.0f}" if x>0 else "—")
    df["credit_fmt"]=df["credit"].apply(lambda x:f"{x:,.0f}" if x>0 else "—")
    mois_opts=[{"label":m,"value":m} for m in sorted(df["date"].dt.to_period("M").astype(str).unique())]
    cat_opts=[{"label":c,"value":c} for c in sorted(df["categorie"].unique())]
    cols=[{"name":"ID","id":"id"},{"name":"Date","id":"date_str"},{"name":"Libelle","id":"libelle"},
          {"name":"Debit","id":"debit_fmt"},{"name":"Credit","id":"credit_fmt"},
          {"name":"Categorie","id":"categorie"},{"name":"Statut","id":"statut"},{"name":"Ref","id":"ref"}]
    data=df[["id","date_str","libelle","debit_fmt","credit_fmt","categorie","statut","ref"]].to_dict("records")
    st=tbl_hdr()
    st["style_data_conditional"]=[
        {"if":{"row_index":"odd"},"backgroundColor":"#21262D"},
        {"if":{"column_id":"debit_fmt"},"color":"#FF7B72","fontWeight":"600"},
        {"if":{"column_id":"credit_fmt"},"color":"#3FB950","fontWeight":"600"},
    ]
    return html.Div([
        html.Div([html.Div("Transactions bancaires",className="page-title",style={"display":"inline"}),
                  html.Span(SOURCE_BQ,className="src-tag")],style={"marginBottom":"18px"}),
        html.Div([
            html.Div([html.Label("Periode",className="flabel"),
                      dcc.Dropdown(mois_opts,id="f-mois",multi=True,placeholder="Tous")],className="filter-group"),
            html.Div([html.Label("Categorie",className="flabel"),
                      dcc.Dropdown(cat_opts,id="f-cat",multi=True,placeholder="Toutes")],className="filter-group"),
            html.Div([html.Label("Type",className="flabel"),
                      dcc.RadioItems([{"label":"Tous","value":"tous"},{"label":"Debits","value":"debit"},
                                      {"label":"Credits","value":"credit"}],
                                     value="tous",id="f-type",className="radio-grp",inline=True)],
                     className="filter-group"),
            html.Div([html.Label("Recherche",className="flabel"),
                      dcc.Input(id="f-search",type="text",placeholder="🔍 libelle, reference...",
                                className="srch",debounce=True,style={"width":"220px"})],className="filter-group"),
        ],className="filter-bar"),
        html.Div([
            html.Div(f"📋 {len(df)} transaction(s)  |  Debits: {df['debit'].sum():,.0f} FCFA  |  Credits: {df['credit'].sum():,.0f} FCFA",
                     id="txn-count",className="table-count"),
            dash_table.DataTable(id="txn-table",columns=cols,data=data,page_size=12,
                page_action="native",sort_action="native",row_selectable="multi",selected_rows=[],**st),
        ],className="table-wrapper"),
        html.Div([
            html.Span("Actions:",className="flabel"),
            html.Button("✅ Marquer Rapproche",id="btn-rappr",className="btn btn-g"),
            html.Button("⏳ En attente",id="btn-attente",className="btn btn-o"),
            html.Button("📤 Exporter CSV",id="btn-csv",className="btn btn-b"),
            dcc.Download(id="dl-csv"),
            html.Span(id="txn-fb",className="afb"),
        ],className="action-bar"),
    ])

# ── PAGE RAPPROCHEMENT ──────────────────────────────────────────
def build_rapprochement():
    taux=TAUX_RAPPR; nb_r=len(DF_RAPPR); nb_nb=len(DF_NON_BQ); nb_nc=len(DF_NON_CPT)
    fig_g=go.Figure(go.Indicator(
        mode="gauge+number",value=taux,
        number={"suffix":"%","font":{"size":34,"color":"#E6EDF3","family":"Courier New"}},
        gauge={"axis":{"range":[0,100],"tickcolor":"#7D8590","tickfont":{"color":"#7D8590","size":9}},
               "bar":{"color":"#58A6FF","thickness":.25},"bgcolor":"#21262D","borderwidth":0,
               "steps":[{"range":[0,60],"color":"rgba(255,123,114,.12)"},
                        {"range":[60,80],"color":"rgba(210,153,34,.12)"},
                        {"range":[80,100],"color":"rgba(63,185,80,.12)"}],
               "threshold":{"line":{"color":"#3FB950","width":3},"thickness":.85,"value":80}},
        title={"text":"Taux de rapprochement","font":{"size":11,"color":"#7D8590"}},
    ))
    fig_g.update_layout(**PLOTLY_BASE,height=240)
    if not DF_RAPPR.empty:
        df_r=DF_RAPPR.copy()
        df_r["Montant"]=df_r["Montant"].apply(lambda x:f"{x:,.0f}")
        cols_r=[{"name":c,"id":c} for c in df_r.columns]; data_r=df_r.to_dict("records")
    else:
        cols_r=[{"name":"Info","id":"info"}]; data_r=[{"info":"Aucune transaction rapprochee"}]
    st_r=tbl_hdr("#1E8449")
    st_r["style_data_conditional"]=[{"if":{"row_index":"odd"},"backgroundColor":"#0D2B1A"},
                                     {"if":{"column_id":"Statut"},"color":"#3FB950","fontWeight":"700"}]
    def mini_tbl(df_in,bg,titre):
        if df_in.empty:
            return html.Div([html.Div(titre,className="card-title"),
                             html.Div("✅ Aucune transaction non rapprochee",
                                      style={"padding":"16px","color":"#3FB950","fontFamily":"monospace","fontSize":"12px"})],
                            className="chart-card")
        cols_s=[c for c in ["date","libelle","montant","ref","categorie"] if c in df_in.columns]
        ds=df_in[cols_s].copy()
        if "date" in ds.columns: ds["date"]=pd.to_datetime(ds["date"]).dt.strftime("%d/%m/%Y")
        if "montant" in ds.columns: ds["montant"]=ds["montant"].apply(lambda x:f"{x:,.0f}")
        st=tbl_hdr("#7B241C"); st["style_data_conditional"]=[{"if":{"row_index":"odd"},"backgroundColor":"#1A0A0A"}]
        return html.Div([html.Div(titre,className="card-title"),
                         dash_table.DataTable(columns=[{"name":c.title(),"id":c} for c in ds.columns],
                             data=ds.to_dict("records"),page_size=8,sort_action="native",**st)],
                        className="chart-card")
    return html.Div([
        html.Div([html.Div("Rapprochement bancaire",className="page-title",style={"display":"inline"}),
                  html.Span(f"BQ:{SOURCE_BQ}  CPT:{SOURCE_CPT}",className="src-tag")],
                 style={"marginBottom":"18px"}),
        html.Div([
            html.Div([dcc.Graph(figure=fig_g,config={"displayModeBar":False})],className="chart-card f1"),
            html.Div([html.Div("Resume",className="card-title"),
                      html.Div([
                          html.Div([html.Div(str(nb_r),className="rappr-num",style={"color":"#3FB950"}),html.Div("Rapprochees",className="rappr-lbl")]),
                          html.Div([html.Div(str(nb_nb),className="rappr-num",style={"color":"#FF7B72"}),html.Div("Non rappr. BQ",className="rappr-lbl")]),
                          html.Div([html.Div(str(nb_nc),className="rappr-num",style={"color":"#D29922"}),html.Div("Non rappr. CPT",className="rappr-lbl")]),
                      ],className="rappr-stat"),
                      html.Div(className="prog-bar",children=[
                          html.Div(className="prog-fill",style={"width":f"{taux:.1f}%",
                              "background":"linear-gradient(90deg,#3FB950,#58A6FF)"})]),
                      html.Div(f"{taux:.1f}% de rapprochement automatique",
                               style={"textAlign":"center","fontSize":"11px","color":"#7D8590",
                                      "fontFamily":"monospace","paddingBottom":"12px"}),
                      ],className="chart-card f1"),
        ],className="charts-row"),
        html.Div([html.Div(f"✅ Transactions rapprochees ({nb_r})",className="card-title"),
                  dash_table.DataTable(columns=cols_r,data=data_r,page_size=8,sort_action="native",**st_r)],
                 className="chart-card",style={"marginBottom":"14px"}),
        html.Div([mini_tbl(DF_NON_BQ,"#7B241C",f"❌ Non rapprochees — Banque ({nb_nb})"),
                  mini_tbl(DF_NON_CPT,"#6E2C00",f"⚠️ Non rapprochees — Comptabilite ({nb_nc})")],
                 className="charts-row"),
        html.Div([html.Button("🔄 Relancer rapprochement",id="btn-rerun",className="btn btn-g"),
                  html.Button("📤 Exporter resultats",id="btn-exp-r",className="btn btn-b"),
                  html.Span(id="rappr-fb",className="afb")],className="action-bar"),
    ])

# ── PAGE ANOMALIES ──────────────────────────────────────────────
def build_anomalies():
    anomalies=[]
    for _,r in DF_NON_BQ.iterrows():
        anomalies.append({"id":f"ANO-BQ-{r['id']}","severite":"Haute",
            "type":"Sans contrepartie comptable","date":r["date"].strftime("%d/%m/%Y"),
            "libelle":str(r["libelle"])[:45],"montant":f"{abs(r['montant']):,.0f}",
            "detail":"Transaction bancaire sans ecriture comptable correspondante","statut":"A traiter"})
    for _,r in DF_NON_CPT.iterrows():
        anomalies.append({"id":f"ANO-CPT-{r['id']}","severite":"Moyenne",
            "type":"Ecriture sans mouvement bancaire","date":r["date"].strftime("%d/%m/%Y"),
            "libelle":str(r["libelle"])[:45],"montant":f"{abs(r['montant']):,.0f}",
            "detail":"Ecriture comptable sans contrepartie bancaire","statut":"A traiter"})
    if not DF_RAPPR.empty:
        for _,r in DF_RAPPR[DF_RAPPR["Ecart jours"]>0].iterrows():
            mt=r["Montant"]
            anomalies.append({"id":f"ANO-DT-{len(anomalies)+1:03d}","severite":"Faible",
                "type":"Decalage de date","date":r["Date Banque"],
                "libelle":str(r["Libelle Banque"])[:45],
                "montant":f"{abs(float(mt)):,.0f}" if isinstance(mt,(int,float)) else str(mt),
                "detail":f"Ecart de {r['Ecart jours']} jour(s) banque/comptabilite","statut":"En cours"})
    for _,r in DF_BQ[DF_BQ["montant"].abs()>2_000_000].iterrows():
        anomalies.append({"id":f"ANO-MT-{r['id']}","severite":"Faible",
            "type":"Montant eleve","date":r["date"].strftime("%d/%m/%Y"),
            "libelle":str(r["libelle"])[:45],"montant":f"{abs(r['montant']):,.0f}",
            "detail":"Montant > 2 000 000 FCFA — verifier justificatif","statut":"En cours"})
    df_a=pd.DataFrame(anomalies) if anomalies else pd.DataFrame(
        columns=["id","severite","type","date","libelle","montant","detail","statut"])
    hautes=(df_a["severite"]=="Haute").sum() if not df_a.empty else 0
    moys=(df_a["severite"]=="Moyenne").sum() if not df_a.empty else 0
    faibles=(df_a["severite"]=="Faible").sum() if not df_a.empty else 0
    fig_s=go.Figure(go.Pie(labels=["Haute","Moyenne","Faible"],
        values=[max(hautes,0),max(moys,0),max(faibles,0)],hole=.6,
        marker=dict(colors=["#FF7B72","#D29922","#3FB950"],line=dict(color="#0D1117",width=2)),
        textinfo="value+percent",hovertemplate="<b>%{label}</b>: %{value}<extra></extra>"))
    fig_s.add_annotation(text=f"<b>{len(df_a)}</b>",x=.5,y=.5,showarrow=False,
        font=dict(size=22,color="#E6EDF3"))
    fig_s.update_layout(**{**PLOTLY_BASE,"legend":dict(bgcolor="rgba(0,0,0,0)",bordercolor="#30363D",font=dict(size=9))},height=230,showlegend=True,
        title=dict(text="Par severite",font=dict(size=12,color="#7D8590")))
    tc=df_a["type"].value_counts() if not df_a.empty else pd.Series(dtype=int)
    fig_t=go.Figure(go.Bar(x=tc.values,y=tc.index,orientation="h",
        marker=dict(color="#58A6FF",line=dict(width=0)),text=tc.values,textposition="outside",
        textfont=dict(color="#E6EDF3",size=10),hovertemplate="%{y}: <b>%{x}</b><extra></extra>"))
    fig_t.update_layout(**PLOTLY_BASE,height=230,
        title=dict(text="Par type",font=dict(size=12,color="#7D8590")),
        xaxis=dict(showgrid=False),yaxis=dict(showgrid=False))
    st=tbl_hdr("#6E2C00")
    st["style_data_conditional"]=[
        {"if":{"row_index":"odd"},"backgroundColor":"#21262D"},
        {"if":{"filter_query":'{'"'severite'"'} = "Haute"',   "column_id":"severite"},"color":"#FF7B72","fontWeight":"700"},
        {"if":{"filter_query":'{'"'severite'"'} = "Moyenne"', "column_id":"severite"},"color":"#D29922","fontWeight":"700"},
        {"if":{"filter_query":'{'"'severite'"'} = "Faible"',  "column_id":"severite"},"color":"#3FB950","fontWeight":"700"},
        {"if":{"filter_query":'{'"'statut'"'} = "A traiter"', "column_id":"statut"},  "color":"#FF7B72"},
        {"if":{"filter_query":'{'"'statut'"'} = "Resolu"',    "column_id":"statut"},  "color":"#3FB950"},
    ]
    return html.Div([
        html.Div("Detection des anomalies",className="page-title"),
        html.Div([kpi("Hautes",str(hautes),"Action urgente","#FF7B72","🔴"),
                  kpi("Moyennes",str(moys),"A investiguer","#D29922","🟡"),
                  kpi("Faibles",str(faibles),"A surveiller","#3FB950","🟢"),
                  kpi("A traiter",str((df_a["statut"]=="A traiter").sum()) if not df_a.empty else "0","Ouvertes","#58A6FF","📌")],
                 className="kpi-grid"),
        html.Div([html.Div([dcc.Graph(figure=fig_s,config={"displayModeBar":False})],className="chart-card f1"),
                  html.Div([dcc.Graph(figure=fig_t,config={"displayModeBar":False})],className="chart-card f2")],
                 className="charts-row"),
        html.Div([html.Div(f"Liste complete ({len(df_a)} anomalies)",className="card-title"),
                  dash_table.DataTable(columns=[{"name":c.title(),"id":c} for c in df_a.columns] if not df_a.empty else [],
                      data=df_a.to_dict("records"),page_size=10,sort_action="native",filter_action="native",**st)],
                 className="chart-card"),
    ])

# ── PAGE ANALYTIQUE ─────────────────────────────────────────────
def build_analytique():
    df=DF_BQ.copy()
    df["heure"]=df["date"].dt.hour; df["jour"]=df["date"].dt.day_name()
    jours_en=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    jours_fr=["Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi","Dimanche"]
    pivot=df.groupby(["jour","heure"])["montant"].count().unstack(fill_value=0)
    pivot=pivot.reindex([j for j in jours_en if j in pivot.index])
    fig_h=go.Figure(go.Heatmap(z=pivot.values,
        x=[f"{h:02d}h" for h in pivot.columns],
        y=[jours_fr[jours_en.index(j)] for j in pivot.index],
        colorscale=[[0,"#21262D"],[.5,"#1F6FEB"],[1,"#58A6FF"]],showscale=False,
        hovertemplate="<b>%{y} %{x}</b><br>%{z} operations<extra></extra>"))
    fig_h.update_layout(**PLOTLY_BASE,height=200,
        title=dict(text="Activite par jour & heure",font=dict(size=12,color="#7D8590")))
    df_s=df.sort_values("date")
    df_s["cumul_cr"]=df_s["credit"].cumsum(); df_s["cumul_db"]=df_s["debit"].cumsum()
    fig_cum=go.Figure()
    fig_cum.add_trace(go.Scatter(x=df_s["date"],y=df_s["cumul_cr"],name="Cumul Credits",
        line=dict(color="#3FB950",width=2),fill="tozeroy",fillcolor="rgba(63,185,80,.07)"))
    fig_cum.add_trace(go.Scatter(x=df_s["date"],y=df_s["cumul_db"],name="Cumul Debits",
        line=dict(color="#FF7B72",width=2),fill="tozeroy",fillcolor="rgba(255,123,114,.07)"))
    fig_cum.update_layout(**PLOTLY_BASE,height=200,
        title=dict(text="Cumul Credits vs Debits",font=dict(size=12,color="#7D8590")))
    pal=["#58A6FF","#3FB950","#F0883E","#FF7B72","#D2A8FF","#D29922","#79C0FF","#56D364"]
    fig_sc=go.Figure()
    for i,cat in enumerate(df["categorie"].unique()):
        sub=df[df["categorie"]==cat]
        fig_sc.add_trace(go.Scatter(x=sub["date"],y=sub["montant"].abs(),mode="markers",name=cat[:18],
            marker=dict(color=pal[i%len(pal)],size=9,opacity=.8,line=dict(width=.5,color="#0D1117")),
            hovertemplate="<b>%{text}</b><br>%{y:,.0f} FCFA<extra></extra>",
            text=sub["libelle"].str[:30]))
    fig_sc.update_layout(**{**PLOTLY_BASE,"legend":dict(bgcolor="rgba(0,0,0,0)",bordercolor="#30363D",font=dict(size=8))},height=260,showlegend=True,
        title=dict(text="Distribution des montants",font=dict(size=12,color="#7D8590")))
    top_db=df[df["debit"]>0].nlargest(10,"debit")
    fig_top=go.Figure(go.Bar(y=[l[:24]+"..." if len(str(l))>24 else l for l in top_db["libelle"]],
        x=top_db["debit"],orientation="h",marker=dict(color="#FF7B72",line=dict(width=0)),
        hovertemplate="%{y}<br><b>%{x:,.0f} FCFA</b><extra></extra>",
        text=top_db["debit"].apply(fmt_m),textposition="outside",textfont=dict(color="#E6EDF3",size=10)))
    fig_top.update_layout(**PLOTLY_BASE,height=260,
        title=dict(text="Top 10 — Plus gros debits",font=dict(size=12,color="#7D8590")),
        xaxis=dict(showgrid=False),yaxis=dict(showgrid=False))
    return html.Div([
        html.Div("Analytique avancee",className="page-title"),
        html.Div([html.Div([dcc.Graph(figure=fig_h,config={"displayModeBar":False})],className="chart-card f2"),
                  html.Div([dcc.Graph(figure=fig_cum,config={"displayModeBar":False})],className="chart-card f2")],
                 className="charts-row"),
        html.Div([html.Div([dcc.Graph(figure=fig_sc,config={"displayModeBar":False})],className="chart-card f3"),
                  html.Div([dcc.Graph(figure=fig_top,config={"displayModeBar":False})],className="chart-card f2")],
                 className="charts-row"),
    ])

# ── PAGE PARAMETRES ─────────────────────────────────────────────
def build_parametres():
    return html.Div([
        html.Div("Parametres & Import",className="page-title"),
        html.Div([
            html.Div([html.Div("Configuration generale",className="card-title"),
                html.Div([
                    html.Div([html.Div("Entreprise",className="param-lbl"),dcc.Input(value=ENTREPRISE,className="srch",style={"width":"280px"})],className="param-row"),
                    html.Div([html.Div("Devise",className="param-lbl"),dcc.Input(value=DEVISE,className="srch",style={"width":"120px"})],className="param-row"),
                    html.Div([html.Div(["Seuil rapprochement (%)",html.Div("Score minimum matching",className="param-sub")],className="param-lbl"),
                              dcc.Slider(60,100,5,value=80,marks={60:"60",70:"70",80:"80",90:"90",100:"100"},tooltip={"always_visible":True})],className="param-row"),
                    html.Div([html.Div(["Fenetre temporelle (jours)",html.Div("Tolerance decalage date",className="param-sub")],className="param-lbl"),
                              dcc.Slider(1,10,1,value=5,marks={1:"1",3:"3",5:"5",7:"7",10:"10"},tooltip={"always_visible":True})],className="param-row"),
                ])],className="param-section f1"),
            html.Div([html.Div("Sources de donnees chargees",className="card-title"),
                html.Div([
                    html.Div([html.Div("📄 Releve bancaire",className="param-lbl"),html.Div(SOURCE_BQ,className="param-sub")],className="param-row"),
                    html.Div([html.Div("📒 Export comptable",className="param-lbl"),html.Div(SOURCE_CPT,className="param-sub")],className="param-row"),
                    html.Div([html.Div(f"Transactions BQ: {len(DF_BQ)}",className="param-lbl"),html.Div(f"Ecritures CPT: {len(DF_CPT)}",className="param-sub")],className="param-row"),
                    html.Div([html.Div(f"Rapprochees: {len(DF_RAPPR)}",className="param-lbl",style={"color":"#3FB950"}),
                              html.Div(f"Ecarts: {len(DF_NON_BQ)+len(DF_NON_CPT)}",className="param-sub",style={"color":"#FF7B72"})],className="param-row"),
                ]),
                html.Div("📂 Importer de nouveaux fichiers",className="card-title"),
                dcc.Upload(id="upload-data",multiple=True,children=html.Div([
                    html.Div("📂",style={"fontSize":"32px","marginBottom":"6px"}),
                    html.Div("Glissez vos fichiers ici",style={"fontWeight":"600"}),
                    html.Div("releve_bancaire.xlsx  |  export_sage.xlsx  |  .csv",
                             style={"fontSize":"10px","color":"#7D8590","marginTop":"4px"}),
                ],style={"textAlign":"center"}),className="upload-zone"),
                html.Div(id="upload-status",style={"padding":"0 16px 12px","fontSize":"11px","color":"#7D8590","fontFamily":"monospace"}),
            ],className="param-section f1"),
        ],className="charts-row"),
        html.Div([
            html.Button("💾 Sauvegarder config",id="btn-save-cfg",className="btn btn-g"),
            html.Button("🔄 Recharger donnees",id="btn-reload",className="btn btn-b"),
            html.Span(id="cfg-fb",className="afb"),
        ],className="action-bar"),
    ])

NAV=[("📊","Tableau de bord","dashboard"),("📋","Transactions","transactions"),
     ("🔗","Rapprochement","rapprochement"),("⚠️","Anomalies","anomalies"),
     ("📈","Analytique","analytique"),("⚙️","Parametres","parametres")]
PAGES={"dashboard":build_dashboard,"transactions":build_transactions,
       "rapprochement":build_rapprochement,"anomalies":build_anomalies,
       "analytique":build_analytique,"parametres":build_parametres}

app=dash.Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP],
              suppress_callback_exceptions=True,title=APP_TITLE)
app.index_string=(
    "<!DOCTYPE html><html><head>{%metas%}<title>{%title%}</title>"
    "{%favicon%}{%css%}<style>"+CSS+"</style></head>"
    "<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body></html>"
)

app.layout=html.Div([
    dcc.Store(id="active-page",data="dashboard"),
    html.Div([
        html.Div([
            html.Div([html.Div("CB",className="logo-mark"),
                      html.Div([html.Div(APP_TITLE,className="logo-title"),html.Div(ENTREPRISE,className="logo-sub")])],
                     className="sidebar-logo"),
            html.Div(className="sidebar-divider"),
            html.Nav([html.Button([html.Span(ic,className="nav-icon"),html.Span(lbl)],
                                   id=f"nav-{pid}",className="nav-item",n_clicks=0)
                      for ic,lbl,pid in NAV],className="sidebar-nav"),
            html.Div(className="sidebar-divider"),
            html.Div([html.Div("● CONNECTE",style={"color":"#3FB950","fontSize":"10px","fontFamily":"monospace","letterSpacing":"1px"}),
                      html.Div(datetime.now().strftime("%d/%m/%Y %H:%M"),style={"color":"#7D8590","fontSize":"10px","fontFamily":"monospace"})],
                     className="sidebar-status"),
        ],className="sidebar"),
        html.Div([
            html.Div([
                html.Div(id="header-title",className="header-title",children="Tableau de bord"),
                html.Div([html.Div(f"BQ:{len(DF_BQ)}tx",className="hbadge"),
                          html.Div(f"CPT:{len(DF_CPT)}ecr",className="hbadge"),
                          html.Div(f"Rappr:{TAUX_RAPPR:.0f}%",className="hbadge",style={"color":"#3FB950","borderColor":"rgba(63,185,80,.3)"}),
                          html.Div("● LIVE",className="hbadge",style={"color":"#3FB950","borderColor":"rgba(63,185,80,.3)"})],
                         className="header-actions"),
            ],className="top-header"),
            html.Div(id="page-container",children=build_dashboard(),style={"padding":"20px 24px","flex":"1"}),
        ],className="main-area"),
    ],className="app-shell"),
],style={"fontFamily":"'Inter',sans-serif"})

@app.callback(
    Output("page-container","children"),
    Output("header-title","children"),
    Output("active-page","data"),
    [Input(f"nav-{pid}","n_clicks") for _,_,pid in NAV],
    prevent_initial_call=True,
)
def naviguer(*args):
    ctx=callback_context
    if not ctx.triggered: return no_update,no_update,no_update
    pid=ctx.triggered[0]["prop_id"].split(".")[0].replace("nav-","")
    titres={p:l for _,l,p in NAV}
    return PAGES.get(pid,build_dashboard)(),titres.get(pid,"Dashboard"),pid

@app.callback(
    [Output(f"nav-{pid}","className") for _,_,pid in NAV],
    Input("active-page","data"),
)
def maj_nav(active):
    return ["nav-item nav-active" if pid==active else "nav-item" for _,_,pid in NAV]

@app.callback(
    Output("txn-table","data"),Output("txn-count","children"),
    Input("f-mois","value"),Input("f-cat","value"),
    Input("f-type","value"),Input("f-search","value"),
    prevent_initial_call=True,
)
def filtrer(mois,cats,type_txn,search):
    df=DF_BQ.copy(); df["mois"]=df["date"].dt.to_period("M").astype(str)
    if mois: df=df[df["mois"].isin(mois)]
    if cats: df=df[df["categorie"].isin(cats)]
    if type_txn=="debit":  df=df[df["debit"]>0]
    if type_txn=="credit": df=df[df["credit"]>0]
    if search:
        m=df["libelle"].str.lower().str.contains(search.lower(),na=False)|df["ref"].str.lower().str.contains(search.lower(),na=False)
        df=df[m]
    df["date_str"]=df["date"].dt.strftime("%d/%m/%Y")
    df["debit_fmt"]=df["debit"].apply(lambda x:f"{x:,.0f}" if x>0 else "—")
    df["credit_fmt"]=df["credit"].apply(lambda x:f"{x:,.0f}" if x>0 else "—")
    cnt=f"📋 {len(df)} tx  |  Debits: {df['debit'].sum():,.0f} FCFA  |  Credits: {df['credit'].sum():,.0f} FCFA"
    return df[["id","date_str","libelle","debit_fmt","credit_fmt","categorie","statut","ref"]].to_dict("records"),cnt

@app.callback(Output("dl-csv","data"),Input("btn-csv","n_clicks"),prevent_initial_call=True)
def export_csv(n):
    df=DF_BQ.copy(); df["date"]=df["date"].dt.strftime("%d/%m/%Y")
    return dcc.send_data_frame(df.to_csv,f"releve_{datetime.now().strftime('%Y%m%d')}.csv",
                               index=False,sep=";",encoding="utf-8-sig")

@app.callback(Output("txn-fb","children"),
    Input("btn-rappr","n_clicks"),Input("btn-attente","n_clicks"),
    State("txn-table","selected_rows"),prevent_initial_call=True)
def action_sel(n1,n2,sel):
    if not sel: return "⚠️ Aucune ligne selectionnee"
    btn=callback_context.triggered[0]["prop_id"]; n=len(sel)
    return f"✅ {n} ligne(s) marquee(s) Rapproche" if "rappr" in btn else f"⏳ {n} ligne(s) En attente"

@app.callback(Output("rappr-fb","children"),Input("btn-rerun","n_clicks"),prevent_initial_call=True)
def rerun(n): return f"✅ Recalcule — {TAUX_RAPPR:.1f}% ({len(DF_RAPPR)} paires)"

@app.callback(Output("upload-status","children"),Input("upload-data","filename"),prevent_initial_call=True)
def on_upload(fns):
    if not fns: return ""
    fns=fns if isinstance(fns,list) else [fns]
    return "  |  ".join([f"✅ {f}" if f.rsplit('.',1)[-1].lower() in ('csv','xlsx','xls') else f"⚠️ {f} — non supporte" for f in fns])

@app.callback(Output("cfg-fb","children"),Input("btn-save-cfg","n_clicks"),prevent_initial_call=True)
def save_cfg(n): return "✅ Configuration sauvegardee"

if __name__=="__main__":
    print("="*60)
    print(f"  {APP_TITLE} — {ENTREPRISE}")
    print(f"  BQ  : {SOURCE_BQ}")
    print(f"  CPT : {SOURCE_CPT}")
    print(f"  Rapprochement : {TAUX_RAPPR:.1f}%  ({len(DF_RAPPR)} paires)")
    print(f"  Ecarts : {len(DF_NON_BQ)+len(DF_NON_CPT)}")
    print(f"  → http://localhost:8050")
    print("="*60)
    app.run(debug=False,host="0.0.0.0",port=8050)