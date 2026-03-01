"""
Subfield definitions and keyword patterns for classification.

Structure: discipline_slug -> subfield_slug -> {display_name, keywords}
Keywords are matched against title + abstract (case-insensitive).
"""

SUBFIELD_KEYWORDS = {
    # ============================================
    # SOCIAL SCIENCES
    # ============================================
    "political-science": {
        "american-politics": {
            "display_name": "American Politics",
            "keywords": [
                "congress", "senate", "house of representatives", "u.s. election",
                "american voter", "presidential", "federal government", "partisan",
                "democrat", "republican", "primary election", "midterm", "gerrymandering",
                "filibuster", "supreme court", "american public opinion", "lobbying",
                "interest group", "pacs", "campaign finance", "roll call", "cosponsorship",
                "legislator", "lawmaker", "politician", "governor", "mayor", "state legislature",
                "ballot", "voter turnout", "political party", "caucus",
            ],
        },
        "comparative-politics": {
            "display_name": "Comparative Politics",
            "keywords": [
                "cross-national", "comparative analysis", "regime type", "democratization",
                "authoritarianism", "electoral system", "parliamentary", "presidentialism",
                "coalition government", "party system", "ethnic politics", "civil war",
                "state capacity", "political development", "clientelism", "patronage",
                "country", "nation", "government", "political regime", "dictatorship",
                "autocracy", "transition", "protest", "revolution",
            ],
        },
        "international-relations": {
            "display_name": "International Relations",
            "keywords": [
                "international", "foreign policy", "diplomacy", "alliance", "treaty",
                "interstate", "sovereignty", "united nations", "nato", "sanctions",
                "nuclear", "arms control", "deterrence", "security dilemma", "hegemony",
                "globalization", "international institution", "interstate conflict",
                "terrorism", "peacekeeping", "humanitarian intervention",
                "war", "conflict", "military", "security", "defense", "cooperation",
            ],
        },
        "political-theory": {
            "display_name": "Political Theory",
            "keywords": [
                "normative", "justice", "legitimacy", "democracy theory", "liberalism",
                "republicanism", "deliberative democracy", "rawls", "political philosophy",
                "citizenship", "rights", "freedom", "equality", "political obligation",
                "democratic", "liberty", "consent", "authority", "power",
            ],
        },
        "political-methodology": {
            "display_name": "Political Methodology",
            "keywords": [
                "causal inference", "regression discontinuity", "difference-in-differences",
                "instrumental variable", "matching", "synthetic control", "survey experiment",
                "text analysis", "machine learning", "bayesian", "measurement",
                "identification strategy", "natural experiment", "quasi-experiment",
            ],
        },
    },

    "economics": {
        "microeconomics": {
            "display_name": "Microeconomics",
            "keywords": [
                "consumer choice", "utility", "demand", "supply", "price elasticity",
                "market structure", "monopoly", "oligopoly", "perfect competition",
                "game theory", "mechanism design", "auction", "contract theory",
                "incentive", "moral hazard", "adverse selection", "principal-agent",
                "firm", "producer", "cost", "profit", "price", "market", "equilibrium",
                "competition", "pricing", "consumer", "buyer", "seller", "trade-off",
            ],
        },
        "macroeconomics": {
            "display_name": "Macroeconomics",
            "keywords": [
                "gdp", "inflation", "unemployment", "business cycle", "monetary policy",
                "fiscal policy", "aggregate demand", "aggregate supply", "recession",
                "economic growth", "central bank", "interest rate", "dsge",
                "economy", "growth", "output", "investment", "consumption", "savings",
                "exchange rate", "currency", "debt", "deficit", "stimulus",
            ],
        },
        "econometrics": {
            "display_name": "Econometrics",
            "keywords": [
                "causal inference", "regression discontinuity", "difference-in-differences",
                "instrumental variable", "panel data", "time series", "gmm",
                "maximum likelihood", "bootstrap", "heteroskedasticity",
                "regression", "estimation", "identification", "endogeneity", "fixed effects",
                "treatment effect", "propensity score", "standard error", "robust",
            ],
        },
        "labor-economics": {
            "display_name": "Labor Economics",
            "keywords": [
                "wage", "employment", "labor market", "minimum wage", "union",
                "human capital", "education return", "job search", "unemployment insurance",
                "labor supply", "occupational choice", "discrimination",
                "worker", "job", "occupation", "earnings", "income", "hiring", "firing",
                "workforce", "skill", "training", "career", "salary",
            ],
        },
        "public-economics": {
            "display_name": "Public Economics",
            "keywords": [
                "taxation", "tax", "public good", "public finance", "welfare",
                "social insurance", "government spending", "redistribution",
                "optimal taxation", "tax evasion", "fiscal federalism",
                "subsidy", "transfer", "benefit", "pension", "social security",
            ],
        },
        "international-economics": {
            "display_name": "International Economics",
            "keywords": [
                "trade", "export", "import", "tariff", "globalization", "outsourcing",
                "foreign direct investment", "multinational", "trade policy",
                "comparative advantage", "trade agreement", "wto", "nafta",
            ],
        },
        "energy-economics": {
            "display_name": "Energy Economics",
            "keywords": [
                "energy", "oil", "gas", "electricity", "renewable", "solar", "wind",
                "fossil fuel", "carbon", "emission", "power", "fuel", "petroleum",
            ],
        },
        "health-economics": {
            "display_name": "Health Economics",
            "keywords": [
                "healthcare", "health insurance", "hospital", "physician", "medical cost",
                "pharmaceutical", "drug pricing", "medicare", "medicaid", "health spending",
            ],
        },
        "development-economics": {
            "display_name": "Development Economics",
            "keywords": [
                "developing country", "poverty", "microfinance", "rct", "randomized",
                "field experiment", "aid", "economic development", "growth",
                "institution", "corruption", "rule of law",
            ],
        },
        "behavioral-economics": {
            "display_name": "Behavioral Economics",
            "keywords": [
                "behavioral", "nudge", "heuristic", "bias", "prospect theory",
                "loss aversion", "time preference", "present bias", "mental accounting",
                "framing", "default", "choice architecture",
            ],
        },
        "finance": {
            "display_name": "Finance",
            "keywords": [
                "asset pricing", "stock", "bond", "portfolio", "risk premium",
                "capm", "market efficiency", "corporate finance", "capital structure",
                "dividend", "ipo", "merger", "venture capital", "private equity",
                "bank", "banking", "credit", "loan", "lending", "borrowing", "deposit",
                "financial", "investment", "investor", "return", "shareholder", "equity",
                "valuation", "profitability", "liquidity", "leverage",
            ],
        },
    },

    "sociology": {
        "social-stratification": {
            "display_name": "Social Stratification",
            "keywords": [
                "inequality", "social mobility", "class", "socioeconomic status",
                "intergenerational", "wealth gap", "income distribution", "poverty",
                "racial", "gender gap", "disparity", "equity", "stratification",
            ],
        },
        "cultural-sociology": {
            "display_name": "Cultural Sociology",
            "keywords": [
                "culture", "identity", "symbolic", "meaning-making", "ritual",
                "boundary", "taste", "cultural capital", "habitus",
                "cultural", "norm", "value", "belief", "ethnicity",
            ],
        },
        "urban-sociology": {
            "display_name": "Urban Sociology",
            "keywords": [
                "urban", "city", "neighborhood", "gentrification", "segregation",
                "housing", "community", "metropolitan",
            ],
        },
        "sociology-of-education": {
            "display_name": "Sociology of Education",
            "keywords": [
                "educational attainment", "school", "college", "tracking",
                "achievement gap", "dropout", "teacher", "curriculum",
            ],
        },
        "medical-sociology": {
            "display_name": "Medical Sociology",
            "keywords": [
                "health disparities", "illness", "healthcare", "medicalization",
                "patient", "disability", "mental health", "chronic disease",
            ],
        },
    },

    "psychology": {
        "cognitive-psychology": {
            "display_name": "Cognitive Psychology",
            "keywords": [
                "memory", "attention", "perception", "reasoning", "decision making",
                "problem solving", "cognitive load", "working memory", "executive function",
                "cognitive", "visual perception", "inhibition", "response time",
                "reaction time", "priming", "recognition",
            ],
        },
        "social-psychology": {
            "display_name": "Social Psychology",
            "keywords": [
                "attitude", "persuasion", "conformity", "group dynamics", "stereotype",
                "prejudice", "intergroup", "social influence", "self-concept",
            ],
        },
        "developmental-psychology": {
            "display_name": "Developmental Psychology",
            "keywords": [
                "child development", "adolescent", "lifespan", "attachment",
                "cognitive development", "language acquisition", "parenting",
            ],
        },
        "clinical-psychology": {
            "display_name": "Clinical Psychology",
            "keywords": [
                "depression", "anxiety", "therapy", "psychotherapy", "mental disorder",
                "treatment", "ptsd", "schizophrenia", "diagnosis", "intervention",
                "disorder", "mental health", "wellbeing", "burnout", "stress",
                "trauma", "substance", "addiction",
            ],
        },
        "organizational-psychology": {
            "display_name": "Organizational Psychology",
            "keywords": [
                "workplace", "leadership", "motivation", "job satisfaction",
                "organizational behavior", "team", "employee", "performance",
            ],
        },
    },

    "public-policy": {
        "policy-analysis": {
            "display_name": "Policy Analysis",
            "keywords": [
                "policy evaluation", "cost-benefit", "impact assessment", "program evaluation",
                "evidence-based", "policy design", "implementation",
                "policy", "public policy", "government program", "intervention",
            ],
        },
        "public-administration": {
            "display_name": "Public Administration",
            "keywords": [
                "bureaucracy", "public management", "civil service", "government agency",
                "public sector", "administrative reform", "e-government",
                "public service", "administration", "government official",
            ],
        },
        "regulatory-policy": {
            "display_name": "Regulatory Policy",
            "keywords": [
                "regulation", "deregulation", "regulatory", "compliance", "enforcement",
                "rulemaking", "administrative law",
                "regulator", "rule", "law", "legal requirement",
            ],
        },
        "social-policy": {
            "display_name": "Social Policy",
            "keywords": [
                "welfare state", "social security", "healthcare policy", "education policy",
                "housing policy", "poverty alleviation", "social protection",
                "social program", "benefit", "assistance", "aid",
            ],
        },
    },

    "law": {
        "constitutional-law": {
            "display_name": "Constitutional Law",
            "keywords": [
                "constitution", "constitutional", "judicial review", "separation of powers",
                "federalism", "bill of rights", "due process", "equal protection",
            ],
        },
        "criminal-law": {
            "display_name": "Criminal Law",
            "keywords": [
                "crime", "criminal", "punishment", "sentencing", "prosecution",
                "defendant", "incarceration", "prison", "police",
            ],
        },
        "contract-law": {
            "display_name": "Contract Law",
            "keywords": [
                "contract", "breach", "damages", "performance", "agreement",
                "consideration", "promissory", "commercial law",
            ],
        },
        "international-law": {
            "display_name": "International Law",
            "keywords": [
                "international law", "treaty", "customary law", "jurisdiction",
                "human rights law", "wto", "international tribunal",
            ],
        },
        "legal-theory": {
            "display_name": "Legal Theory",
            "keywords": [
                "jurisprudence", "legal philosophy", "natural law", "positivism",
                "legal realism", "rule of law", "legal interpretation",
            ],
        },
    },

    "communications": {
        "media-studies": {
            "display_name": "Media Studies",
            "keywords": [
                "media", "news", "journalism", "press", "broadcast", "television",
                "newspaper", "media effects", "agenda setting",
            ],
        },
        "political-communication": {
            "display_name": "Political Communication",
            "keywords": [
                "political advertising", "campaign", "political news", "propaganda",
                "misinformation", "fact-checking", "political rhetoric",
            ],
        },
        "digital-media": {
            "display_name": "Digital Media",
            "keywords": [
                "social media", "twitter", "facebook", "online", "internet",
                "viral", "platform", "algorithm", "digital",
            ],
        },
        "public-opinion-comm": {
            "display_name": "Public Opinion",
            "keywords": [
                "public opinion", "polling", "survey", "attitude", "perception",
                "trust", "polarization",
            ],
        },
    },

    "education": {
        "higher-education": {
            "display_name": "Higher Education",
            "keywords": [
                "university", "college", "higher education", "enrollment",
                "graduation", "tuition", "financial aid", "faculty",
                "student", "academic", "campus", "degree", "professor",
            ],
        },
        "k12-education": {
            "display_name": "K-12 Education",
            "keywords": [
                "school", "elementary", "secondary", "high school", "middle school",
                "teacher", "principal", "classroom", "curriculum",
                "pupil", "grade", "instruction", "lesson", "educational",
            ],
        },
        "educational-psychology": {
            "display_name": "Educational Psychology",
            "keywords": [
                "learning", "motivation", "self-efficacy", "metacognition",
                "student engagement", "academic achievement",
                "learner", "teaching", "instruction", "cognitive development",
            ],
        },
    },

    "business": {
        "management": {
            "display_name": "Management",
            "keywords": [
                "management", "strategy", "competitive advantage", "firm performance",
                "organizational", "ceo", "executive", "governance",
            ],
        },
        "marketing": {
            "display_name": "Marketing",
            "keywords": [
                "marketing", "consumer", "brand", "advertising", "pricing",
                "customer", "purchase", "retail",
            ],
        },
        "operations": {
            "display_name": "Operations",
            "keywords": [
                "supply chain", "operations", "inventory", "logistics", "manufacturing",
                "quality", "production", "process",
            ],
        },
        "entrepreneurship": {
            "display_name": "Entrepreneurship",
            "keywords": [
                "entrepreneur", "startup", "venture", "innovation", "new venture",
                "founder", "small business",
            ],
        },
        "accounting": {
            "display_name": "Accounting",
            "keywords": [
                "accounting", "audit", "financial reporting", "gaap", "ifrs",
                "earnings management", "accrual", "disclosure", "auditor",
                "tax", "bookkeeping", "balance sheet", "revenue recognition",
            ],
        },
    },

    # ============================================
    # HUMANITIES
    # ============================================
    "history": {
        "ancient-history": {
            "display_name": "Ancient History",
            "keywords": [
                "ancient", "classical", "rome", "roman", "greece", "greek", "egypt",
                "mesopotamia", "antiquity", "hellenistic",
            ],
        },
        "medieval-history": {
            "display_name": "Medieval History",
            "keywords": [
                "medieval", "middle ages", "feudal", "crusade", "byzantine",
                "carolingian", "viking", "monastery",
            ],
        },
        "modern-history": {
            "display_name": "Modern History",
            "keywords": [
                "modern", "19th century", "20th century", "world war", "cold war",
                "industrial", "nationalism", "colonialism", "decolonization",
                "century", "historical", "empire", "colonial", "postwar",
            ],
        },
        "economic-history": {
            "display_name": "Economic History",
            "keywords": [
                "economic history", "historical gdp", "trade history", "industrialization",
                "great depression", "financial crisis", "monetary history",
            ],
        },
        "political-history": {
            "display_name": "Political History",
            "keywords": [
                "political history", "state formation", "revolution", "regime change",
                "diplomacy history", "electoral history", "governance",
            ],
        },
    },

    "philosophy": {
        "ethics": {
            "display_name": "Ethics",
            "keywords": [
                "ethics", "moral", "normative", "ought", "virtue", "duty",
                "consequentialism", "deontology", "utilitarianism",
                "ethical", "bioethics", "applied ethics", "moral responsibility",
            ],
        },
        "epistemology": {
            "display_name": "Epistemology",
            "keywords": [
                "knowledge", "belief", "justification", "skepticism", "epistemology",
                "truth", "evidence", "rationality",
            ],
        },
        "metaphysics": {
            "display_name": "Metaphysics",
            "keywords": [
                "metaphysics", "ontology", "existence", "reality", "modality",
                "causation", "time", "identity", "substance",
            ],
        },
        "philosophy-of-mind": {
            "display_name": "Philosophy of Mind",
            "keywords": [
                "consciousness", "mental", "intentionality", "qualia", "phenomenal",
                "physicalism", "dualism", "mind-body",
            ],
        },
        "political-philosophy": {
            "display_name": "Political Philosophy",
            "keywords": [
                "justice", "liberty", "equality", "rights", "democracy",
                "political legitimacy", "social contract", "rawls",
            ],
        },
    },

    "literature": {
        "literary-theory": {
            "display_name": "Literary Theory",
            "keywords": [
                "literary theory", "narrative", "poststructuralism", "deconstruction",
                "reader response", "new criticism", "formalism",
            ],
        },
        "british-literature": {
            "display_name": "British Literature",
            "keywords": [
                "british literature", "english literature", "victorian", "romantic",
                "shakespeare", "modernist", "postcolonial",
            ],
        },
        "comparative-literature": {
            "display_name": "Comparative Literature",
            "keywords": [
                "comparative literature", "translation", "world literature",
                "transnational", "intertextuality",
            ],
        },
    },

    "linguistics": {
        "syntax": {
            "display_name": "Syntax",
            "keywords": [
                "syntax", "syntactic", "phrase structure", "movement", "binding",
                "minimalism", "grammatical",
            ],
        },
        "phonology": {
            "display_name": "Phonology",
            "keywords": [
                "phonology", "phonological", "phoneme", "prosody", "intonation",
                "stress", "syllable", "tone",
            ],
        },
        "semantics": {
            "display_name": "Semantics",
            "keywords": [
                "semantics", "semantic", "meaning", "reference", "truth condition",
                "compositionality", "lexical",
            ],
        },
        "sociolinguistics": {
            "display_name": "Sociolinguistics",
            "keywords": [
                "sociolinguistic", "dialect", "language variation", "code-switching",
                "language attitude", "language policy", "language shift",
            ],
        },
        "psycholinguistics": {
            "display_name": "Psycholinguistics",
            "keywords": [
                "language processing", "sentence processing", "word recognition",
                "language acquisition", "second language", "bilingual",
            ],
        },
    },

    "art-history": {
        "renaissance-art": {
            "display_name": "Renaissance Art",
            "keywords": [
                "renaissance", "quattrocento", "cinquecento", "michelangelo",
                "leonardo", "raphael", "florentine",
            ],
        },
        "modern-art": {
            "display_name": "Modern Art",
            "keywords": [
                "modernism", "impressionism", "expressionism", "cubism", "surrealism",
                "abstract", "avant-garde",
            ],
        },
        "contemporary-art": {
            "display_name": "Contemporary Art",
            "keywords": [
                "contemporary art", "installation", "performance art", "conceptual art",
                "postmodern", "video art",
            ],
        },
        "art-theory": {
            "display_name": "Art Theory",
            "keywords": [
                "aesthetics", "visual culture", "representation", "gaze", "spectacle",
                "art criticism", "iconography",
            ],
        },
    },

    "religious-studies": {
        "biblical-studies": {
            "display_name": "Biblical Studies",
            "keywords": [
                "bible", "biblical", "new testament", "old testament", "gospel",
                "hebrew bible", "pauline", "scriptural",
            ],
        },
        "theology": {
            "display_name": "Theology",
            "keywords": [
                "theology", "theological", "doctrine", "christology", "ecclesiology",
                "soteriology", "eschatology",
            ],
        },
        "comparative-religion": {
            "display_name": "Comparative Religion",
            "keywords": [
                "comparative religion", "world religion", "interfaith", "religious pluralism",
                "syncretism",
            ],
        },
        "religion-and-society": {
            "display_name": "Religion and Society",
            "keywords": [
                "secularization", "religious practice", "religious identity", "ritual",
                "pilgrimage", "sacred", "religious movement",
            ],
        },
    },

    # ============================================
    # NATURAL SCIENCES
    # ============================================
    "physics": {
        "particle-physics": {
            "display_name": "Particle Physics",
            "keywords": [
                "particle", "hadron", "quark", "lepton", "higgs", "lhc", "collider",
                "standard model", "neutrino", "antimatter",
            ],
        },
        "condensed-matter": {
            "display_name": "Condensed Matter",
            "keywords": [
                "condensed matter", "solid state", "superconductor", "semiconductor",
                "magnetic", "phase transition", "crystal", "phonon",
                "topological", "spin", "lattice", "band structure",
                "ferromagnetic", "antiferromagnetic", "dielectric",
            ],
        },
        "astrophysics": {
            "display_name": "Astrophysics",
            "keywords": [
                "astrophysics", "cosmology", "galaxy", "star", "black hole", "pulsar",
                "dark matter", "dark energy", "gravitational wave",
            ],
        },
        "quantum-physics": {
            "display_name": "Quantum Physics",
            "keywords": [
                "quantum", "entanglement", "superposition", "qubit", "quantum computing",
                "quantum information", "decoherence",
            ],
        },
        "optics": {
            "display_name": "Optics",
            "keywords": [
                "optical", "laser", "photon", "fiber", "spectroscopy", "imaging",
                "nonlinear optics", "plasmon",
                "waveguide", "detector", "infrared", "ultraviolet", "lens",
            ],
        },
    },

    "chemistry": {
        "organic-chemistry": {
            "display_name": "Organic Chemistry",
            "keywords": [
                "organic", "synthesis", "reaction mechanism", "catalysis", "asymmetric",
                "natural product", "total synthesis",
                "molecule", "compound", "chemical", "reagent", "oxidation", "reduction",
            ],
        },
        "inorganic-chemistry": {
            "display_name": "Inorganic Chemistry",
            "keywords": [
                "inorganic", "coordination", "transition metal", "organometallic",
                "ligand", "catalyst", "metal complex",
                "crystal", "structure", "oxide", "ion", "ionic",
            ],
        },
        "physical-chemistry": {
            "display_name": "Physical Chemistry",
            "keywords": [
                "physical chemistry", "thermodynamics", "kinetics", "spectroscopy",
                "quantum chemistry", "molecular dynamics",
                "energy", "reaction rate", "equilibrium", "activation",
            ],
        },
        "analytical-chemistry": {
            "display_name": "Analytical Chemistry",
            "keywords": [
                "analytical", "chromatography", "mass spectrometry", "nmr", "detection",
                "separation", "sensor",
                "spectroscopic", "analysis", "measurement", "concentration",
            ],
        },
        "biochemistry": {
            "display_name": "Biochemistry",
            "keywords": [
                "biochemistry", "enzyme", "protein", "metabolic", "biosynthesis",
                "nucleic acid", "lipid",
                "amino acid", "peptide", "biological", "cell", "molecular",
            ],
        },
    },

    "biology": {
        "molecular-biology": {
            "display_name": "Molecular Biology",
            "keywords": [
                "molecular", "dna", "rna", "transcription", "translation", "gene expression",
                "crispr", "pcr",
            ],
        },
        "cell-biology": {
            "display_name": "Cell Biology",
            "keywords": [
                "cell", "cellular", "membrane", "organelle", "cytoskeleton", "signaling",
                "apoptosis", "mitosis",
                "tissue", "organ", "blood", "platelet", "vessel", "vascular",
            ],
        },
        "genetics": {
            "display_name": "Genetics",
            "keywords": [
                "genetic", "genome", "mutation", "heredity", "mendelian", "gwas",
                "polygenic", "heritability",
            ],
        },
        "ecology": {
            "display_name": "Ecology",
            "keywords": [
                "ecology", "ecosystem", "biodiversity", "population", "community",
                "habitat", "species interaction", "food web",
            ],
        },
        "evolutionary-biology": {
            "display_name": "Evolutionary Biology",
            "keywords": [
                "evolution", "natural selection", "adaptation", "phylogenetic", "speciation",
                "fitness", "evolutionary",
            ],
        },
        "neuroscience": {
            "display_name": "Neuroscience",
            "keywords": [
                "neuron", "brain", "neural", "synaptic", "cortex", "hippocampus",
                "neurotransmitter", "cognitive neuroscience",
            ],
        },
    },

    "earth-sciences": {
        "geology": {
            "display_name": "Geology",
            "keywords": [
                "geological", "rock", "mineral", "tectonic", "volcanic", "sediment",
                "earthquake", "fault",
            ],
        },
        "oceanography": {
            "display_name": "Oceanography",
            "keywords": [
                "ocean", "marine", "sea", "coastal", "current", "tide",
                "deep sea", "coral",
            ],
        },
        "atmospheric-science": {
            "display_name": "Atmospheric Science",
            "keywords": [
                "atmospheric", "climate", "weather", "meteorology", "precipitation",
                "aerosol", "stratosphere",
            ],
        },
        "geophysics": {
            "display_name": "Geophysics",
            "keywords": [
                "geophysics", "seismic", "magnetic field", "gravity", "mantle",
                "core", "plate tectonics",
            ],
        },
    },

    "environmental-science": {
        "climate-science": {
            "display_name": "Climate Science",
            "keywords": [
                "climate change", "global warming", "greenhouse", "carbon", "ipcc",
                "temperature", "emission",
            ],
        },
        "conservation": {
            "display_name": "Conservation",
            "keywords": [
                "conservation", "endangered", "protected area", "wildlife", "habitat loss",
                "restoration", "biodiversity loss",
            ],
        },
        "pollution": {
            "display_name": "Pollution",
            "keywords": [
                "pollution", "contamination", "air quality", "water quality", "waste",
                "remediation", "toxic",
            ],
        },
        "sustainability": {
            "display_name": "Sustainability",
            "keywords": [
                "sustainability", "sustainable development", "renewable", "circular economy",
                "green", "environmental policy",
            ],
        },
    },

    # ============================================
    # ENGINEERING & TECHNOLOGY
    # ============================================
    "computer-science": {
        "artificial-intelligence": {
            "display_name": "Artificial Intelligence",
            "keywords": [
                "artificial intelligence", "ai", "intelligent agent", "planning",
                "reasoning", "knowledge representation",
                "knowledge graph", "multi-agent", "expert system", "decision making",
            ],
        },
        "machine-learning-cs": {
            "display_name": "Machine Learning",
            "keywords": [
                "machine learning", "deep learning", "neural network", "supervised",
                "unsupervised", "reinforcement learning", "transformer", "llm",
                "classification", "regression", "clustering", "prediction",
                "training", "embedding", "feature extraction", "feature selection",
                "contrastive learning", "federated learning", "few-shot", "zero-shot",
                "transfer learning", "meta-learning", "self-supervised",
                "semi-supervised", "knowledge distillation", "data augmentation",
                "generative model", "generative adversarial", "autoencoder",
                "attention mechanism", "self-attention", "pre-training", "fine-tuning",
                "graph neural", "recurrent neural", "convolutional neural",
                "anomaly detection", "recommendation", "benchmark",
                "gradient descent", "backpropagation", "loss function",
            ],
        },
        "computer-vision": {
            "display_name": "Computer Vision",
            "keywords": [
                "computer vision", "image", "object detection", "segmentation",
                "recognition", "visual", "cnn",
                "face detection", "face recognition", "video", "depth estimation",
                "pose estimation", "scene understanding", "3d reconstruction",
                "point cloud", "lidar", "optical flow", "image generation",
            ],
        },
        "natural-language-processing": {
            "display_name": "Natural Language Processing",
            "keywords": [
                "nlp", "natural language", "text", "language model", "parsing",
                "sentiment", "question answering", "translation",
                "text classification", "named entity", "information extraction",
                "summarization", "dialogue", "chatbot", "word embedding",
                "bert", "gpt", "language generation", "text mining", "corpus",
                "token", "speech recognition", "text generation",
            ],
        },
        "databases": {
            "display_name": "Databases",
            "keywords": [
                "database", "query", "sql", "nosql", "data management",
                "indexing", "transaction", "relational", "schema",
                "data warehouse", "olap", "data lake",
            ],
        },
        "networking": {
            "display_name": "Networking",
            "keywords": [
                "network protocol", "routing", "tcp", "ip", "internet",
                "software-defined networking", "sdn", "bandwidth",
                "packet", "latency", "congestion", "network architecture",
            ],
        },
        "distributed-systems": {
            "display_name": "Distributed Systems",
            "keywords": [
                "distributed system", "cloud", "parallel", "concurrency",
                "distributed", "server", "cluster", "blockchain",
                "microservice", "container", "middleware", "scalab",
                "operating system", "storage", "fault tolerance",
                "orchestration", "serverless", "load balancing",
            ],
        },
        "edge-iot": {
            "display_name": "Edge Computing & IoT",
            "keywords": [
                "iot", "internet of things", "edge computing", "fog computing",
                "mobile computing", "task offloading", "uav", "vehicular",
                "resource allocation", "smart city", "smart home",
                "sensor network", "wearable", "embedded system",
            ],
        },
        "algorithms": {
            "display_name": "Algorithms & Optimization",
            "keywords": [
                "algorithm", "complexity", "np-hard", "approximation", "optimization",
                "graph theory", "automata", "computability",
                "combinatorial", "scheduling", "heuristic", "metaheuristic",
                "convergence", "polynomial", "bound",
                "data structure", "sorting", "search algorithm",
            ],
        },
        "information-theory": {
            "display_name": "Information Theory",
            "keywords": [
                "information theory", "coding theory", "entropy", "channel capacity",
                "error correction", "decoding", "encoding",
                "source coding", "rate distortion", "mutual information",
            ],
        },
        "human-computer-interaction": {
            "display_name": "Human-Computer Interaction",
            "keywords": [
                "hci", "user interface", "usability", "user experience", "ux",
                "interaction design", "accessibility",
                "user study", "visualization", "dashboard", "mobile app",
                "crowdsourcing", "annotation",
            ],
        },
        "security-privacy": {
            "display_name": "Security & Privacy",
            "keywords": [
                "security", "privacy", "encryption", "cryptograph", "authentication",
                "malware", "vulnerability", "trojan", "backdoor", "adversarial attack",
                "access control", "intrusion", "threat", "exploit", "forensic",
                "secure", "cipher", "phishing", "ransomware", "firewall",
                "zero-day", "penetration testing", "obfuscation", "tamper",
                "privacy-preserving", "differential privacy", "homomorphic",
            ],
        },
        "software-engineering": {
            "display_name": "Software Engineering",
            "keywords": [
                "software engineering", "software testing", "test case", "bug",
                "debugging", "refactor", "software architecture", "technical debt",
                "code review", "program analysis", "compiler", "program slicing",
                "software quality", "software maintenance", "code generation",
                "code clone", "software repository", "continuous integration",
                "devops", "version control", "code smell",
            ],
        },
        "computer-architecture": {
            "display_name": "Computer Architecture",
            "keywords": [
                "processor", "accelerator", "cache", "memory system",
                "fault tolerance", "throughput", "hardware", "fpga", "gpu", "cpu",
                "pipeline", "ssd", "microarchitecture", "instruction", "register",
                "interconnection", "memory hierarchy", "latency",
                "reconfigurable", "dataflow", "noc", "multicore",
            ],
        },
        "robotics-cs": {
            "display_name": "Robotics (CS)",
            "keywords": [
                "robot learning", "autonomous agent", "robot navigation",
                "robot planning", "human-robot interaction", "drone",
                "autonomous driving", "self-driving", "slam",
                "robot manipulation", "swarm robotics",
            ],
        },
        "information-retrieval": {
            "display_name": "Information Retrieval",
            "keywords": [
                "information retrieval", "search engine", "web search",
                "document retrieval", "ranking", "relevance",
                "recommender system", "collaborative filtering",
                "content-based filtering", "query expansion",
            ],
        },
        "data-mining": {
            "display_name": "Data Mining",
            "keywords": [
                "data mining", "knowledge discovery", "pattern mining",
                "association rule", "frequent pattern", "outlier detection",
                "social network analysis", "graph mining",
                "text mining", "web mining", "stream mining",
            ],
        },
    },

    "electrical-engineering": {
        "power-systems": {
            "display_name": "Power Systems",
            "keywords": [
                "power system", "grid", "renewable energy", "smart grid", "power electronics",
                "transmission", "distribution",
            ],
        },
        "signal-processing": {
            "display_name": "Signal Processing",
            "keywords": [
                "signal processing", "filter", "fourier", "spectral", "compression",
                "audio", "speech",
                "sampling", "noise", "frequency", "wavelet", "denoising",
            ],
        },
        "communications-ee": {
            "display_name": "Communications",
            "keywords": [
                "wireless", "5g", "antenna", "modulation", "channel", "mimo",
                "communication system",
            ],
        },
        "control-systems": {
            "display_name": "Control Systems",
            "keywords": [
                "control system", "feedback", "pid", "optimal control", "robust control",
                "adaptive control", "state estimation",
            ],
        },
        "electronics": {
            "display_name": "Electronics",
            "keywords": [
                "circuit", "transistor", "vlsi", "cmos", "amplifier", "sensor",
                "embedded", "microcontroller",
            ],
        },
    },

    "mechanical-engineering": {
        "thermodynamics-me": {
            "display_name": "Thermodynamics",
            "keywords": [
                "thermodynamics", "heat transfer", "thermal", "combustion", "hvac",
                "refrigeration", "heat exchanger",
            ],
        },
        "fluid-mechanics": {
            "display_name": "Fluid Mechanics",
            "keywords": [
                "fluid", "flow", "turbulent", "aerodynamic", "cfd", "navier-stokes",
                "viscosity", "boundary layer",
            ],
        },
        "solid-mechanics": {
            "display_name": "Solid Mechanics",
            "keywords": [
                "stress", "strain", "fatigue", "fracture", "deformation", "elasticity",
                "plasticity", "finite element",
            ],
        },
        "manufacturing": {
            "display_name": "Manufacturing",
            "keywords": [
                "manufacturing", "machining", "additive manufacturing", "3d printing",
                "cnc", "assembly", "quality control",
            ],
        },
        "robotics": {
            "display_name": "Robotics",
            "keywords": [
                "robot", "robotic", "manipulator", "autonomous", "motion planning",
                "kinematics", "dynamics",
            ],
        },
    },

    "civil-engineering": {
        "structural-engineering": {
            "display_name": "Structural Engineering",
            "keywords": [
                "structural", "beam", "column", "concrete", "steel structure", "bridge",
                "building", "load",
                "reinforced", "frame", "seismic", "stiffness", "strength",
                "composite", "fiber", "shear", "deflection", "masonry",
            ],
        },
        "geotechnical": {
            "display_name": "Geotechnical Engineering",
            "keywords": [
                "geotechnical", "soil", "foundation", "slope", "retaining wall",
                "excavation", "ground",
                "clay", "sand", "pile", "tunnel", "liquefaction",
                "freezing", "embankment", "bearing capacity",
            ],
        },
        "transportation-engineering": {
            "display_name": "Transportation Engineering",
            "keywords": [
                "transportation", "traffic", "highway", "road", "pavement", "transit",
                "vehicle", "autonomous vehicle",
                "railway", "intersection", "pedestrian", "signal", "route",
                "logistics", "freight", "travel time",
            ],
        },
        "water-resources": {
            "display_name": "Water Resources",
            "keywords": [
                "water resource", "hydrology", "dam", "flood", "drainage", "irrigation",
                "groundwater",
                "water", "hydraulic", "river", "aquifer", "runoff", "watershed",
                "streamflow", "porous media", "sediment transport",
            ],
        },
        "construction-management": {
            "display_name": "Construction Management",
            "keywords": [
                "construction", "project management", "scheduling", "cost estimation",
                "bim", "contractor",
                "energy efficiency", "retrofit", "lifecycle", "safety",
                "prefabricated", "modular construction",
            ],
        },
        "construction-materials": {
            "display_name": "Construction Materials",
            "keywords": [
                "concrete", "cement", "morite", "asphalt", "aggregate",
                "admixture", "curing", "compressive strength",
                "fly ash", "slag", "geopolymer", "alkali-activated",
                "durability", "corrosion", "carbonation", "chloride",
            ],
        },
    },

    "materials-science": {
        "metals": {
            "display_name": "Metals & Alloys",
            "keywords": [
                "metal", "alloy", "steel", "aluminum", "titanium", "metallurgy",
                "microstructure",
            ],
        },
        "polymers": {
            "display_name": "Polymers",
            "keywords": [
                "polymer", "plastic", "rubber", "polymerization", "copolymer",
                "viscoelastic", "thermoplastic",
            ],
        },
        "ceramics": {
            "display_name": "Ceramics",
            "keywords": [
                "ceramic", "oxide", "glass", "sintering", "refractory", "porcelain",
            ],
        },
        "nanomaterials": {
            "display_name": "Nanomaterials",
            "keywords": [
                "nano", "nanoparticle", "nanotube", "graphene", "quantum dot",
                "nanostructure", "nanoscale",
            ],
        },
        "biomaterials": {
            "display_name": "Biomaterials",
            "keywords": [
                "biomaterial", "biocompatible", "implant", "scaffold", "tissue engineering",
                "drug delivery",
            ],
        },
    },

    # ============================================
    # HEALTH SCIENCES
    # ============================================
    "medicine": {
        "internal-medicine": {
            "display_name": "Internal Medicine",
            "keywords": [
                "internal medicine", "diagnosis", "treatment", "patient", "clinical",
                "hospital", "physician",
                "disease", "therapy", "drug", "medication", "symptom", "prognosis",
                "inflammation", "infection", "immune", "chronic", "acute",
            ],
        },
        "surgery": {
            "display_name": "Surgery",
            "keywords": [
                "surgery", "surgical", "operation", "laparoscopic", "minimally invasive",
                "postoperative",
            ],
        },
        "oncology": {
            "display_name": "Oncology",
            "keywords": [
                "cancer", "tumor", "oncology", "chemotherapy", "radiation therapy",
                "metastasis", "carcinoma",
            ],
        },
        "cardiology": {
            "display_name": "Cardiology",
            "keywords": [
                "cardiovascular", "heart", "cardiac", "artery", "hypertension",
                "myocardial", "stroke",
                "blood", "vascular", "vessel", "platelet", "bleeding", "clot",
            ],
        },
        "neurology": {
            "display_name": "Neurology",
            "keywords": [
                "neurological", "alzheimer", "parkinson", "epilepsy", "multiple sclerosis",
                "dementia", "stroke",
            ],
        },
    },

    "public-health": {
        "epidemiology": {
            "display_name": "Epidemiology",
            "keywords": [
                "epidemiology", "incidence", "prevalence", "outbreak", "surveillance",
                "risk factor", "cohort",
            ],
        },
        "health-policy": {
            "display_name": "Health Policy",
            "keywords": [
                "health policy", "healthcare system", "insurance", "access to care",
                "health reform", "medicare", "medicaid",
            ],
        },
        "global-health": {
            "display_name": "Global Health",
            "keywords": [
                "global health", "developing country", "infectious disease", "vaccine",
                "who", "pandemic", "malaria",
            ],
        },
    },


    "pharmacology": {
        "drug-discovery": {
            "display_name": "Drug Discovery",
            "keywords": [
                "drug discovery", "drug design", "lead compound", "screening", "target",
                "therapeutic",
            ],
        },
        "pharmacokinetics": {
            "display_name": "Pharmacokinetics",
            "keywords": [
                "pharmacokinetics", "absorption", "distribution", "metabolism", "excretion",
                "bioavailability", "half-life",
            ],
        },
        "clinical-pharmacology": {
            "display_name": "Clinical Pharmacology",
            "keywords": [
                "clinical trial", "drug interaction", "adverse effect", "dosing",
                "efficacy", "safety",
            ],
        },
        "toxicology": {
            "display_name": "Toxicology",
            "keywords": [
                "toxicity", "toxic", "poisoning", "overdose", "carcinogenic", "mutagenic",
            ],
        },
    },

    # ============================================
    # FORMAL SCIENCES
    # ============================================
    "mathematics": {
        "algebra": {
            "display_name": "Algebra",
            "keywords": [
                "algebra", "group", "ring", "field", "module", "representation",
                "homomorphism", "isomorphism",
            ],
        },
        "analysis": {
            "display_name": "Analysis",
            "keywords": [
                "analysis", "measure", "integration", "functional analysis", "harmonic",
                "convergence", "differential equation",
            ],
        },
        "geometry": {
            "display_name": "Geometry",
            "keywords": [
                "geometry", "manifold", "riemannian", "curvature", "metric", "differential geometry",
            ],
        },
        "topology": {
            "display_name": "Topology",
            "keywords": [
                "topology", "topological", "homotopy", "homology", "cohomology",
                "knot", "fiber bundle",
            ],
        },
        "number-theory": {
            "display_name": "Number Theory",
            "keywords": [
                "number theory", "prime", "modular", "diophantine", "elliptic curve",
                "algebraic number",
            ],
        },
        "applied-mathematics": {
            "display_name": "Applied Mathematics",
            "keywords": [
                "applied mathematics", "numerical", "computational", "optimization",
                "simulation", "modeling",
            ],
        },
        "mathematical-logic": {
            "display_name": "Mathematical Logic",
            "keywords": [
                "mathematical logic", "set theory", "model theory", "proof theory",
                "recursion theory", "computability",
            ],
        },
    },

    "statistics": {
        "statistical-theory": {
            "display_name": "Statistical Theory",
            "keywords": [
                "statistical inference", "estimation", "hypothesis testing", "confidence interval",
                "asymptotic", "likelihood",
            ],
        },
        "bayesian-statistics": {
            "display_name": "Bayesian Statistics",
            "keywords": [
                "bayesian", "prior", "posterior", "mcmc", "hierarchical", "credible interval",
            ],
        },
        "biostatistics": {
            "display_name": "Biostatistics",
            "keywords": [
                "biostatistics", "survival analysis", "clinical trial", "meta-analysis",
                "epidemiological",
            ],
        },
        "machine-learning-stat": {
            "display_name": "Statistical Learning",
            "keywords": [
                "statistical learning", "regularization", "cross-validation", "bootstrap",
                "model selection",
            ],
        },
        "causal-inference": {
            "display_name": "Causal Inference",
            "keywords": [
                "causal inference", "causal", "treatment effect", "counterfactual",
                "instrumental variable", "propensity score", "causal discovery",
                "do-calculus", "structural equation", "mediation",
            ],
        },
    },

}


def get_all_subfields() -> list[dict]:
    """Get flat list of all subfields with discipline info."""
    subfields = []
    for discipline_slug, discipline_subfields in SUBFIELD_KEYWORDS.items():
        for subfield_slug, info in discipline_subfields.items():
            subfields.append({
                "discipline_slug": discipline_slug,
                "subfield_slug": subfield_slug,
                "display_name": info["display_name"],
                "keywords": info["keywords"],
            })
    return subfields


def count_subfields() -> dict:
    """Count subfields per discipline."""
    counts = {}
    for discipline_slug, discipline_subfields in SUBFIELD_KEYWORDS.items():
        counts[discipline_slug] = len(discipline_subfields)
    return counts
