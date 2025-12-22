"""
Unit tests for PowerBI user creation fix.

This module tests the fix for the PowerBI user creation bug where
CorpUserKeyClass was being emitted instead of CorpUserInfoClass,
causing existing user data to be overwritten with incomplete data.
"""

from unittest.mock import MagicMock, patch

import pytest
from datahub.metadata.schema_classes import CorpUserInfoClass, CorpUserKeyClass

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    OwnershipMapping,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import User


@pytest.fixture
def mock_pipeline_context():
    """Create a mock pipeline context with graph."""
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = MagicMock()
    return ctx


@pytest.fixture
def mock_pipeline_context_no_graph():
    """Create a mock pipeline context without graph (file-based sink)."""
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = None
    return ctx


@pytest.fixture
def mock_config():
    """Create a mock PowerBI config with default ownership settings."""
    config = MagicMock(spec=PowerBiDashboardSourceConfig)
    config.ownership = OwnershipMapping()
    config.platform_name = "powerbi"
    return config


@pytest.fixture
def mock_reporter():
    """Create a mock reporter."""
    return MagicMock(spec=PowerBiDashboardSourceReport)


@pytest.fixture
def mock_dataplatform_resolver():
    """Create a mock dataplatform instance resolver."""
    return MagicMock(spec=AbstractDataPlatformInstanceResolver)


@pytest.fixture
def sample_user():
    """Create a sample PowerBI user."""
    return User(
        id="user123",
        displayName="John Doe",
        emailAddress="john.doe@company.com",
        graphId="graph-id-123",
        principalType="User",
    )


@pytest.fixture
def sample_user_no_email():
    """Create a sample PowerBI user without email."""
    return User(
        id="user456",
        displayName="Jane Doe",
        emailAddress="",
        graphId="graph-id-456",
        principalType="User",
    )


@pytest.fixture
def sample_app_principal():
    """Create a sample PowerBI App principal (non-human)."""
    return User(
        id="app789",
        displayName="Service App",
        emailAddress="",
        graphId="graph-id-789",
        principalType="App",
    )


class TestCorpUserInfoEmission:
    """Tests verifying CorpUserInfoClass is emitted instead of CorpUserKeyClass."""

    def test_emits_corp_user_info_not_key(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """Verify CorpUserInfoClass is emitted, not CorpUserKeyClass."""
        # Setup: User doesn't exist
        mock_pipeline_context.graph.get_entities.return_value = {}

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(sample_user)

        assert len(mcps) == 1
        assert isinstance(mcps[0].aspect, CorpUserInfoClass)
        assert not isinstance(mcps[0].aspect, CorpUserKeyClass)

    def test_user_info_has_display_name_email_active(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """Verify all fields are populated correctly."""
        mock_pipeline_context.graph.get_entities.return_value = {}

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(sample_user)

        assert len(mcps) == 1
        user_info = mcps[0].aspect
        assert isinstance(user_info, CorpUserInfoClass)
        assert user_info.displayName == "John Doe"
        assert user_info.email == "john.doe@company.com"
        assert user_info.active is True


class TestSkipOverwriteLogic:
    """Tests for skip/overwrite logic based on config and user existence."""

    def test_skips_existing_user_when_overwrite_false(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """User exists + overwrite=False → skip (return empty list)."""
        # User exists
        mock_pipeline_context.graph.get_entities.return_value = {
            "urn:li:corpuser:john.doe@company.com": {
                "corpUserInfo": (MagicMock(spec=CorpUserInfoClass), None)
            }
        }
        mock_config.ownership.overwrite_existing_users = False

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(sample_user)

        assert len(mcps) == 0  # Skipped

    def test_creates_new_user_when_overwrite_false(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """User doesn't exist + overwrite=False → create."""
        mock_pipeline_context.graph.get_entities.return_value = {}
        mock_config.ownership.overwrite_existing_users = False

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(sample_user)

        assert len(mcps) == 1
        assert isinstance(mcps[0].aspect, CorpUserInfoClass)

    def test_overwrites_existing_user_when_overwrite_true(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """User exists + overwrite=True → create."""
        # User exists
        mock_pipeline_context.graph.get_entities.return_value = {
            "urn:li:corpuser:john.doe@company.com": {
                "corpUserInfo": (MagicMock(spec=CorpUserInfoClass), None)
            }
        }
        mock_config.ownership.overwrite_existing_users = True

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(sample_user)

        assert len(mcps) == 1
        assert isinstance(mcps[0].aspect, CorpUserInfoClass)


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_warns_on_file_based_ingestion_no_graph(
        self,
        mock_pipeline_context_no_graph,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """Graph=None + overwrite=False → warn and create."""
        mock_config.ownership.overwrite_existing_users = False

        mapper = Mapper(
            ctx=mock_pipeline_context_no_graph,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        with patch("datahub.ingestion.source.powerbi.powerbi.logger") as mock_logger:
            mcps = mapper.to_datahub_user(sample_user)

            # Should create user despite no graph access
            assert len(mcps) == 1
            assert isinstance(mcps[0].aspect, CorpUserInfoClass)

            # Should log warning
            mock_logger.warning.assert_called()
            warning_call = mock_logger.warning.call_args[0][0]
            assert "overwrite_existing_users=False" in warning_call
            assert "Graph unavailable" in warning_call

    def test_non_human_principal_marked_inactive(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_app_principal,
    ):
        """principalType='App' → active=False."""
        mock_pipeline_context.graph.get_entities.return_value = {}

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(sample_app_principal)

        assert len(mcps) == 1
        user_info = mcps[0].aspect
        assert isinstance(user_info, CorpUserInfoClass)
        assert user_info.active is False  # App principal should be inactive

    def test_falls_back_to_user_id_when_no_display_name(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """displayName='' → uses user.id."""
        user_no_display = User(
            id="user999",
            displayName="",
            emailAddress="test@company.com",
            graphId="graph-id-999",
            principalType="User",
        )
        mock_pipeline_context.graph.get_entities.return_value = {}

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(user_no_display)

        assert len(mcps) == 1
        user_info = mcps[0].aspect
        assert isinstance(user_info, CorpUserInfoClass)
        assert user_info.displayName == "user999"  # Falls back to ID


class TestCustomProperties:
    """Tests for custom properties traceability."""

    def test_custom_properties_populated(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """Verify powerbi_* keys in customProperties."""
        mock_pipeline_context.graph.get_entities.return_value = {}

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(sample_user)

        assert len(mcps) == 1
        user_info = mcps[0].aspect
        assert isinstance(user_info, CorpUserInfoClass)
        assert user_info.customProperties is not None
        assert user_info.customProperties["powerbi_user_id"] == "user123"
        assert user_info.customProperties["powerbi_principal_type"] == "User"
        assert user_info.customProperties["powerbi_graph_id"] == "graph-id-123"

    def test_graph_id_omitted_when_none(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """graphId=None → not in customProperties."""
        user_no_graph_id = User(
            id="user888",
            displayName="Test User",
            emailAddress="test@company.com",
            graphId="",  # Empty string
            principalType="User",
        )
        mock_pipeline_context.graph.get_entities.return_value = {}

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(user_no_graph_id)

        assert len(mcps) == 1
        user_info = mcps[0].aspect
        assert isinstance(user_info, CorpUserInfoClass)
        assert user_info.customProperties is not None
        assert "powerbi_graph_id" not in user_info.customProperties


class TestCaching:
    """Tests for user existence caching."""

    def test_user_existence_cached(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """Second call to _check_user_exists uses cache."""
        mock_pipeline_context.graph.get_entities.return_value = {}
        mock_config.ownership.overwrite_existing_users = False

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        # First call
        mapper._check_user_exists("urn:li:corpuser:test")
        # Second call - should use cache
        mapper._check_user_exists("urn:li:corpuser:test")

        # get_entities should only be called once due to caching
        assert mock_pipeline_context.graph.get_entities.call_count == 1


class TestErrorHandling:
    """Tests for error handling scenarios."""

    def test_logs_warning_on_graph_api_error(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """get_entities() throws → log warning, proceed with creation."""
        mock_pipeline_context.graph.get_entities.side_effect = Exception("API Error")
        mock_config.ownership.overwrite_existing_users = False

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        with patch("datahub.ingestion.source.powerbi.powerbi.logger") as mock_logger:
            mcps = mapper.to_datahub_user(sample_user)

            # Should still create user despite API error
            assert len(mcps) == 1
            assert isinstance(mcps[0].aspect, CorpUserInfoClass)

            # Should log warning about API error
            mock_logger.warning.assert_called()


class TestConfigValidation:
    """Tests for config validation."""

    def test_invalid_config_overwrite_without_create_raises(self):
        """create_corp_user=False + overwrite=True → ValidationError."""
        with pytest.raises(ValueError) as exc_info:
            OwnershipMapping(
                create_corp_user=False,
                overwrite_existing_users=True,
            )

        assert "overwrite_existing_users=True requires create_corp_user=True" in str(
            exc_info.value
        )

    def test_valid_config_create_true_overwrite_false(self):
        """create_corp_user=True + overwrite=False → valid."""
        config = OwnershipMapping(
            create_corp_user=True,
            overwrite_existing_users=False,
        )
        assert config.create_corp_user is True
        assert config.overwrite_existing_users is False

    def test_valid_config_create_false_overwrite_false(self):
        """create_corp_user=False + overwrite=False → valid."""
        config = OwnershipMapping(
            create_corp_user=False,
            overwrite_existing_users=False,
        )
        assert config.create_corp_user is False
        assert config.overwrite_existing_users is False


class TestCreateCorpUserDisabled:
    """Tests for when create_corp_user is disabled."""

    def test_returns_empty_when_create_corp_user_false(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
        sample_user,
    ):
        """create_corp_user=False → returns empty list."""
        mock_config.ownership.create_corp_user = False

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(sample_user)

        assert len(mcps) == 0


class TestUrnConfiguration:
    """Tests for URN configuration options."""

    def test_use_powerbi_email_false_uses_user_id(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """use_powerbi_email=False → URN uses 'users.{id}' format."""
        user = User(
            id="powerbi-user-123",
            displayName="Test User",
            emailAddress="test@company.com",
            graphId="graph-123",
            principalType="User",
        )
        mock_pipeline_context.graph.get_entities.return_value = {}
        mock_config.ownership.use_powerbi_email = False

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(user)

        assert len(mcps) == 1
        # URN should use "users.{id}" format
        assert mcps[0].entityUrn is not None
        assert "users.powerbi-user-123" in mcps[0].entityUrn

    def test_remove_email_suffix_strips_domain(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """remove_email_suffix=True → strips @domain from URN."""
        user = User(
            id="user-456",
            displayName="Test User",
            emailAddress="john.doe@company.com",
            graphId="graph-456",
            principalType="User",
        )
        mock_pipeline_context.graph.get_entities.return_value = {}
        mock_config.ownership.use_powerbi_email = True
        mock_config.ownership.remove_email_suffix = True

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(user)

        assert len(mcps) == 1
        # URN should have email without domain
        assert mcps[0].entityUrn is not None
        assert "john.doe" in mcps[0].entityUrn
        assert "@company.com" not in mcps[0].entityUrn


class TestPrincipalTypes:
    """Tests for different principal types."""

    def test_service_principal_marked_inactive(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """principalType='ServicePrincipal' → active=False."""
        service_principal = User(
            id="sp-123",
            displayName="My Service Principal",
            emailAddress="",
            graphId="graph-sp-123",
            principalType="ServicePrincipal",
        )
        mock_pipeline_context.graph.get_entities.return_value = {}

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(service_principal)

        assert len(mcps) == 1
        user_info = mcps[0].aspect
        assert isinstance(user_info, CorpUserInfoClass)
        assert user_info.active is False

    def test_group_principal_marked_inactive(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """principalType='Group' → active=False."""
        group = User(
            id="group-123",
            displayName="Engineering Team",
            emailAddress="engineering@company.com",
            graphId="graph-group-123",
            principalType="Group",
        )
        mock_pipeline_context.graph.get_entities.return_value = {}

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_user(group)

        assert len(mcps) == 1
        user_info = mcps[0].aspect
        assert isinstance(user_info, CorpUserInfoClass)
        assert user_info.active is False


class TestMultipleUsers:
    """Tests for processing multiple users."""

    def test_cache_prevents_duplicate_api_calls(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """Processing same user twice should only call API once."""
        user1 = User(
            id="user-same",
            displayName="Same User",
            emailAddress="same@company.com",
            graphId="graph-same",
            principalType="User",
        )
        user2 = User(
            id="user-same",  # Same user
            displayName="Same User",
            emailAddress="same@company.com",
            graphId="graph-same",
            principalType="User",
        )
        mock_pipeline_context.graph.get_entities.return_value = {}
        mock_config.ownership.overwrite_existing_users = False

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mapper.to_datahub_user(user1)
        mapper.to_datahub_user(user2)

        # API should only be called once due to caching
        assert mock_pipeline_context.graph.get_entities.call_count == 1


class TestToDatahubUsersMethod:
    """Tests for the to_datahub_users() method which handles multiple users."""

    def test_filters_non_user_principals_when_owner_criteria_set(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """When owner_criteria is set, non-User principals are filtered out."""
        users = [
            User(
                id="user1",
                displayName="Human User",
                emailAddress="user@company.com",
                graphId="graph-1",
                principalType="User",
                dashboardUserAccessRight="Owner",
            ),
            User(
                id="app1",
                displayName="App Principal",
                emailAddress="",
                graphId="graph-2",
                principalType="App",  # Non-user, should be filtered
                dashboardUserAccessRight="Owner",
            ),
        ]
        mock_pipeline_context.graph.get_entities.return_value = {}
        mock_config.ownership.owner_criteria = ["Owner"]

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_users(users)

        # Only the human user should be created (App is filtered by owner_criteria logic)
        assert len(mcps) == 1
        assert mcps[0].entityUrn is not None
        assert "user@company.com" in mcps[0].entityUrn

    def test_creates_all_users_when_no_owner_criteria(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """When owner_criteria is None, all users are created."""
        users = [
            User(
                id="user1",
                displayName="User One",
                emailAddress="user1@company.com",
                graphId="graph-1",
                principalType="User",
            ),
            User(
                id="user2",
                displayName="User Two",
                emailAddress="user2@company.com",
                graphId="graph-2",
                principalType="User",
            ),
        ]
        mock_pipeline_context.graph.get_entities.return_value = {}
        mock_config.ownership.owner_criteria = None  # No filtering

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_users(users)

        assert len(mcps) == 2

    def test_filters_users_without_required_access_rights(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """Users without the required access rights are filtered out."""
        users = [
            User(
                id="owner",
                displayName="Owner User",
                emailAddress="owner@company.com",
                graphId="graph-1",
                principalType="User",
                dashboardUserAccessRight="Owner",
            ),
            User(
                id="viewer",
                displayName="Viewer User",
                emailAddress="viewer@company.com",
                graphId="graph-2",
                principalType="User",
                dashboardUserAccessRight="Viewer",  # Not in owner_criteria
            ),
        ]
        mock_pipeline_context.graph.get_entities.return_value = {}
        mock_config.ownership.owner_criteria = ["Owner", "Admin"]

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_users(users)

        # Only owner should be created (viewer filtered out)
        assert len(mcps) == 1
        assert mcps[0].entityUrn is not None
        assert "owner@company.com" in mcps[0].entityUrn

    def test_handles_none_users_in_list(
        self,
        mock_pipeline_context,
        mock_config,
        mock_reporter,
        mock_dataplatform_resolver,
    ):
        """None values in user list are handled gracefully."""
        users = [
            User(
                id="user1",
                displayName="Valid User",
                emailAddress="valid@company.com",
                graphId="graph-1",
                principalType="User",
            ),
            None,  # Should be skipped
        ]
        mock_pipeline_context.graph.get_entities.return_value = {}
        mock_config.ownership.owner_criteria = None

        mapper = Mapper(
            ctx=mock_pipeline_context,
            config=mock_config,
            reporter=mock_reporter,
            dataplatform_instance_resolver=mock_dataplatform_resolver,
        )

        mcps = mapper.to_datahub_users(users)  # type: ignore[arg-type]

        assert len(mcps) == 1
