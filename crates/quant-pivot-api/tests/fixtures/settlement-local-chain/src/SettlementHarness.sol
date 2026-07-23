// SPDX-License-Identifier: MIT
pragma solidity 0.8.30;

contract MockConditionalTokens {
    mapping(address => mapping(uint256 => uint256)) private balances;
    mapping(address => mapping(address => bool)) private approvals;
    mapping(bytes32 => uint256) public payoutDenominator;
    mapping(bytes32 => mapping(uint256 => uint256)) public payoutNumerators;

    function balanceOf(address account, uint256 id) external view returns (uint256) {
        return balances[account][id];
    }

    function isApprovedForAll(address account, address operator) external view returns (bool) {
        return approvals[account][operator];
    }

    function setApprovalForAll(address operator, bool approved) external {
        approvals[msg.sender][operator] = approved;
    }

    function seedResolvedBinary(bytes32 conditionId) external {
        payoutDenominator[conditionId] = 1;
        payoutNumerators[conditionId][0] = 1;
        payoutNumerators[conditionId][1] = 0;
    }

    function seedBalance(address account, uint256 id, uint256 amount) external {
        balances[account][id] = amount;
    }

    function seedApproval(address account, address operator, bool approved) external {
        approvals[account][operator] = approved;
    }

    function consumeAll(address account, uint256 yesToken, uint256 noToken)
        external
        returns (uint256 yesBalance, uint256 noBalance)
    {
        yesBalance = balances[account][yesToken];
        noBalance = balances[account][noToken];
        balances[account][yesToken] = 0;
        balances[account][noToken] = 0;
    }
}

contract MockPusd {
    mapping(address => uint256) public balanceOf;

    function mint(address account, uint256 amount) external {
        balanceOf[account] += amount;
    }
}

contract MockUsdce {
    mapping(address => uint256) public balanceOf;

    function seedBalance(address account, uint256 amount) external {
        balanceOf[account] = amount;
    }
}

contract MockSettlementAdapter {
    MockConditionalTokens public immutable CONDITIONAL_TOKENS;
    MockPusd public immutable COLLATERAL_TOKEN;
    address public immutable USDCE;
    uint256 public immutable YES_TOKEN;
    uint256 public immutable NO_TOKEN;
    bool private isPaused;

    constructor(
        MockConditionalTokens conditionalTokens,
        MockPusd collateralToken,
        address usdce,
        uint256 yesToken,
        uint256 noToken
    ) {
        CONDITIONAL_TOKENS = conditionalTokens;
        COLLATERAL_TOKEN = collateralToken;
        USDCE = usdce;
        YES_TOKEN = yesToken;
        NO_TOKEN = noToken;
    }

    function paused(address asset) external view returns (bool) {
        return asset == USDCE && isPaused;
    }

    function setPaused(bool value) external {
        isPaused = value;
    }

    function redeemPositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] calldata indexSets
    ) external {
        require(!isPaused, "paused");
        require(collateralToken == address(COLLATERAL_TOKEN), "collateral");
        require(parentCollectionId == bytes32(0), "parent");
        require(indexSets.length == 2 && indexSets[0] == 1 && indexSets[1] == 2, "index sets");
        require(CONDITIONAL_TOKENS.isApprovedForAll(msg.sender, address(this)), "approval");

        uint256 denominator = CONDITIONAL_TOKENS.payoutDenominator(conditionId);
        require(denominator != 0, "unresolved");
        (uint256 yesBalance, uint256 noBalance) =
            CONDITIONAL_TOKENS.consumeAll(msg.sender, YES_TOKEN, NO_TOKEN);
        uint256 payout =
            (yesBalance * CONDITIONAL_TOKENS.payoutNumerators(conditionId, 0)
                + noBalance * CONDITIONAL_TOKENS.payoutNumerators(conditionId, 1)) / denominator;
        COLLATERAL_TOKEN.mint(msg.sender, payout);
    }
}

contract SettlementHarness {
    bytes32 public constant CONDITION =
        0x1111111111111111111111111111111111111111111111111111111111111111;
    uint256 public constant YES_TOKEN = 11;
    uint256 public constant NO_TOKEN = 22;

    MockConditionalTokens public immutable CTF_TEMPLATE;
    MockPusd public immutable PUSD_TEMPLATE;
    MockUsdce public immutable USDCE_TEMPLATE;
    MockSettlementAdapter public standard;
    MockSettlementAdapter public negRisk;

    constructor() {
        CTF_TEMPLATE = new MockConditionalTokens();
        PUSD_TEMPLATE = new MockPusd();
        USDCE_TEMPLATE = new MockUsdce();
    }

    function initialize(address funder, address ctfAddress, address pusdAddress, address usdce)
        external
    {
        require(address(standard) == address(0), "initialized");
        MockConditionalTokens ctf = MockConditionalTokens(ctfAddress);
        MockPusd pusd = MockPusd(pusdAddress);
        standard = new MockSettlementAdapter(ctf, pusd, usdce, YES_TOKEN, NO_TOKEN);
        negRisk = new MockSettlementAdapter(ctf, pusd, usdce, YES_TOKEN, NO_TOKEN);
        ctf.seedResolvedBinary(CONDITION);
        seed(funder, address(standard), ctfAddress);
        ctf.seedApproval(funder, address(negRisk), true);
    }

    function seed(address funder, address adapter, address ctfAddress) public {
        MockConditionalTokens ctf = MockConditionalTokens(ctfAddress);
        ctf.seedBalance(funder, YES_TOKEN, 12_500_000);
        ctf.seedBalance(funder, NO_TOKEN, 3_000_000);
        ctf.seedApproval(funder, adapter, true);
    }
}
